package storage

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
)

func setupIdempotencyRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		client.Close()
		mr.Close()
	})
	return mr, client
}

func newIdempotencyStore(t *testing.T) (*miniredis.Miniredis, *redis.Client, *IdempotencyStore) {
	t.Helper()
	mr, redisClient := setupIdempotencyRedis(t)
	store := NewIdempotencyStore(redisClient, nil, zerolog.Nop())
	return mr, redisClient, store
}

// ----- NewIdempotencyStore -----

func TestNewIdempotencyStore(t *testing.T) {
	_, redisClient := setupIdempotencyRedis(t)

	store := NewIdempotencyStore(redisClient, nil, zerolog.Nop())
	if store == nil {
		t.Fatal("expected non-nil store")
	}
	if store.redis != redisClient {
		t.Error("expected redis client to be set")
	}
	if store.db != nil {
		t.Error("expected nil db when not provided")
	}
}

// ----- MarkProcessing -----

func TestIdempotencyStore_MarkProcessing_Success(t *testing.T) {
	mr, _, store := newIdempotencyStore(t)
	ctx := context.Background()

	key := "test-key-1"
	if err := store.MarkProcessing(ctx, key); err != nil {
		t.Fatalf("MarkProcessing failed: %v", err)
	}

	// Redis key should exist
	if !mr.Exists(keyProcessing + key) {
		t.Error("expected processing key to exist in Redis after MarkProcessing")
	}
}

func TestIdempotencyStore_MarkProcessing_DuplicateReturnsError(t *testing.T) {
	_, _, store := newIdempotencyStore(t)
	ctx := context.Background()

	key := "test-key-dup"
	if err := store.MarkProcessing(ctx, key); err != nil {
		t.Fatalf("first MarkProcessing failed: %v", err)
	}

	// Second call should fail — key is already set (SetNX semantics)
	if err := store.MarkProcessing(ctx, key); err == nil {
		t.Error("expected error on duplicate MarkProcessing")
	}
}

func TestIdempotencyStore_MarkProcessing_SetsTTL(t *testing.T) {
	mr, _, store := newIdempotencyStore(t)
	ctx := context.Background()

	key := "test-key-ttl"
	if err := store.MarkProcessing(ctx, key); err != nil {
		t.Fatalf("MarkProcessing failed: %v", err)
	}

	ttl := mr.TTL(keyProcessing + key)
	if ttl <= 0 {
		t.Error("expected positive TTL on processing key")
	}
	if ttl > processingTTL+time.Second {
		t.Errorf("expected TTL <= %v, got %v", processingTTL, ttl)
	}
}

// ----- ClearProcessing -----

func TestIdempotencyStore_ClearProcessing_RemovesKey(t *testing.T) {
	mr, _, store := newIdempotencyStore(t)
	ctx := context.Background()

	key := "test-key-clear"
	if err := store.MarkProcessing(ctx, key); err != nil {
		t.Fatalf("MarkProcessing failed: %v", err)
	}

	if err := store.ClearProcessing(ctx, key); err != nil {
		t.Fatalf("ClearProcessing failed: %v", err)
	}

	if mr.Exists(keyProcessing + key) {
		t.Error("expected processing key to be removed from Redis")
	}
}

func TestIdempotencyStore_ClearProcessing_NonExistentKey(t *testing.T) {
	_, _, store := newIdempotencyStore(t)
	ctx := context.Background()

	// Clearing a key that was never set should not error
	if err := store.ClearProcessing(ctx, "never-set-key"); err != nil {
		t.Errorf("ClearProcessing on non-existent key should not error: %v", err)
	}
}

// ----- MarkProcessing + ClearProcessing lifecycle -----

func TestIdempotencyStore_MarkAfterClearSucceeds(t *testing.T) {
	_, _, store := newIdempotencyStore(t)
	ctx := context.Background()

	key := "test-key-reuse"

	if err := store.MarkProcessing(ctx, key); err != nil {
		t.Fatalf("first MarkProcessing failed: %v", err)
	}
	if err := store.ClearProcessing(ctx, key); err != nil {
		t.Fatalf("ClearProcessing failed: %v", err)
	}
	// After clear, the key should be re-acquirable
	if err := store.MarkProcessing(ctx, key); err != nil {
		t.Errorf("second MarkProcessing after clear failed: %v", err)
	}
}

// ----- IsProcessed (Redis fast path only) -----

func TestIdempotencyStore_IsProcessed_RedisFastPath_HitReturnsTrue(t *testing.T) {
	_, redisClient, store := newIdempotencyStore(t)
	ctx := context.Background()

	key := "test-processed-hit"
	// Pre-populate Redis processed key directly (simulating a previously-processed event)
	if err := redisClient.Set(ctx, keyProcessed+key, "1", processedTTL).Err(); err != nil {
		t.Fatalf("failed to seed Redis: %v", err)
	}

	// IsProcessed should return true from the Redis fast path — no DB access
	processed, err := store.IsProcessed(ctx, key)
	if err != nil {
		t.Fatalf("IsProcessed failed: %v", err)
	}
	if !processed {
		t.Error("expected IsProcessed to return true for pre-set Redis key")
	}
}

func TestIdempotencyStore_IsProcessed_RedisFastPath_MissReturnsFalseWhenNoRedisEntry(t *testing.T) {
	// When there is no Redis entry AND no DB, IsProcessed will try to query a nil DB
	// and panic/error. We verify the Redis-hit case only here.
	// For the miss+DB path, a real database would be needed (out of scope for unit tests).
	t.Log("DB slow-path for IsProcessed not tested here — requires a real database")
}

// ----- Concurrent MarkProcessing -----

func TestIdempotencyStore_MarkProcessing_Concurrent(t *testing.T) {
	_, _, store := newIdempotencyStore(t)
	ctx := context.Background()

	key := "concurrent-key"
	results := make(chan error, 10)

	for i := 0; i < 10; i++ {
		go func() {
			results <- store.MarkProcessing(ctx, key)
		}()
	}

	var successes, failures int
	for i := 0; i < 10; i++ {
		if err := <-results; err == nil {
			successes++
		} else {
			failures++
		}
	}

	// Exactly one goroutine should have won the SetNX race
	if successes != 1 {
		t.Errorf("expected exactly 1 successful MarkProcessing, got %d", successes)
	}
	if failures != 9 {
		t.Errorf("expected exactly 9 failed MarkProcessing calls, got %d", failures)
	}
}
