package storage

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return mr, client
}

func TestNewRedisCache(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")

	if cache == nil {
		t.Fatal("expected non-nil cache")
	}
	if cache.client != client {
		t.Error("expected client to be set")
	}
	if cache.prefix != "test" {
		t.Errorf("expected prefix 'test', got '%s'", cache.prefix)
	}
}

func TestRedisCache_BuildKey(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	tests := []struct {
		name     string
		prefix   string
		key      string
		expected string
	}{
		{"with prefix", "myapp", "mykey", "myapp:mykey"},
		{"empty prefix", "", "mykey", "mykey"},
		{"nested key", "app", "level1:level2", "app:level1:level2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewRedisCache(client, tt.prefix)
			result := cache.buildKey(tt.key)
			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestRedisCache_SetAndGet(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Test Set
	err := cache.Set(ctx, "key1", "value1", 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Test Get
	value, err := cache.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("expected 'value1', got '%s'", value)
	}
}

func TestRedisCache_GetNonExistent(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Get non-existent key should return empty string, not error
	value, err := cache.Get(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Get should not error for non-existent key: %v", err)
	}
	if value != "" {
		t.Errorf("expected empty string for non-existent key, got '%s'", value)
	}
}

func TestRedisCache_SetWithTTL(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Set with 1 second TTL
	err := cache.Set(ctx, "expiring", "value", 1)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify key exists
	value, err := cache.Get(ctx, "expiring")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != "value" {
		t.Errorf("expected 'value', got '%s'", value)
	}

	// Fast forward time in miniredis
	mr.FastForward(2 * time.Second)

	// Key should be expired now
	value, err = cache.Get(ctx, "expiring")
	if err != nil {
		t.Fatalf("Get failed after expiry: %v", err)
	}
	if value != "" {
		t.Errorf("expected empty string after expiry, got '%s'", value)
	}
}

func TestRedisCache_Delete(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Set a key
	err := cache.Set(ctx, "todelete", "value", 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify it exists
	value, _ := cache.Get(ctx, "todelete")
	if value != "value" {
		t.Fatalf("expected key to exist before delete")
	}

	// Delete it
	err = cache.Delete(ctx, "todelete")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	value, _ = cache.Get(ctx, "todelete")
	if value != "" {
		t.Errorf("expected empty string after delete, got '%s'", value)
	}
}

func TestRedisCache_DeleteNonExistent(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Delete non-existent key should not error
	err := cache.Delete(ctx, "nonexistent")
	if err != nil {
		t.Errorf("Delete should not error for non-existent key: %v", err)
	}
}

func TestRedisCache_DeleteByPrefix(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "app")
	ctx := context.Background()

	// Set multiple keys with different prefixes
	cache.Set(ctx, "queue:url:queue1", "url1", 0)
	cache.Set(ctx, "queue:url:queue2", "url2", 0)
	cache.Set(ctx, "queue:url:queue3", "url3", 0)
	cache.Set(ctx, "other:key1", "value1", 0)
	cache.Set(ctx, "other:key2", "value2", 0)

	// Delete all keys with "queue:url:" prefix
	err := cache.DeleteByPrefix(ctx, "queue:url:")
	if err != nil {
		t.Fatalf("DeleteByPrefix failed: %v", err)
	}

	// Verify queue:url keys are deleted
	for _, key := range []string{"queue:url:queue1", "queue:url:queue2", "queue:url:queue3"} {
		value, _ := cache.Get(ctx, key)
		if value != "" {
			t.Errorf("expected key '%s' to be deleted, but got value '%s'", key, value)
		}
	}

	// Verify other keys still exist
	for _, key := range []string{"other:key1", "other:key2"} {
		value, _ := cache.Get(ctx, key)
		if value == "" {
			t.Errorf("expected key '%s' to still exist", key)
		}
	}
}

func TestRedisCache_DeleteByPrefixEmpty(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Delete by prefix with no matching keys should not error
	err := cache.DeleteByPrefix(ctx, "nonexistent:")
	if err != nil {
		t.Errorf("DeleteByPrefix should not error when no keys match: %v", err)
	}
}

func TestRedisCache_Exists(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Check non-existent key
	exists, err := cache.Exists(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected non-existent key to return false")
	}

	// Set a key
	cache.Set(ctx, "exists", "value", 0)

	// Check existing key
	exists, err = cache.Exists(ctx, "exists")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected existing key to return true")
	}
}

func TestRedisCache_Ping(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	err := cache.Ping(ctx)
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestRedisCache_PingFailure(t *testing.T) {
	// Create a client with invalid address
	client := redis.NewClient(&redis.Options{
		Addr: "invalid:6379",
	})
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	err := cache.Ping(ctx)
	if err == nil {
		t.Error("expected Ping to fail with invalid address")
	}
}

func TestRedisCache_DefaultTTL(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Set with TTL=0 should use default TTL (24 hours)
	err := cache.Set(ctx, "defaultttl", "value", 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Check the TTL is set (should be around 24 hours = 86400 seconds)
	ttl := mr.TTL("test:defaultttl")
	if ttl < 86000*time.Second || ttl > 86400*time.Second {
		t.Errorf("expected TTL around 24 hours, got %v", ttl)
	}
}

func TestRedisCache_Concurrency(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Concurrent writes
	done := make(chan bool, 200)
	for i := 0; i < 100; i++ {
		go func(i int) {
			key := "key"
			cache.Set(ctx, key, "value", 0)
			done <- true
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 100; i++ {
		go func() {
			cache.Get(ctx, "key")
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 200; i++ {
		<-done
	}

	// Final value should be set
	value, err := cache.Get(ctx, "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != "value" {
		t.Errorf("expected 'value', got '%s'", value)
	}
}

func TestRedisCache_SpecialCharacters(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	tests := []struct {
		name  string
		key   string
		value string
	}{
		{"with colon", "key:with:colons", "value"},
		{"with spaces", "key with spaces", "value with spaces"},
		{"with unicode", "key-日本語", "値-日本語"},
		{"with special chars", "key!@#$%", "value!@#$%"},
		{"url value", "queue-url", "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cache.Set(ctx, tt.key, tt.value, 0)
			if err != nil {
				t.Fatalf("Set failed: %v", err)
			}

			value, err := cache.Get(ctx, tt.key)
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}
			if value != tt.value {
				t.Errorf("expected '%s', got '%s'", tt.value, value)
			}
		})
	}
}

func TestRedisCache_ImplementsInterface(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")

	// This is a compile-time check that RedisCache implements contracts.Cache
	// If it doesn't, this file won't compile
	_ = cache
}

func TestRedisCache_EmptyValue(t *testing.T) {
	mr, client := setupTestRedis(t)
	defer mr.Close()
	defer client.Close()

	cache := NewRedisCache(client, "test")
	ctx := context.Background()

	// Set empty value
	err := cache.Set(ctx, "emptyvalue", "", 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get should return empty string (which is the set value)
	value, err := cache.Get(ctx, "emptyvalue")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != "" {
		t.Errorf("expected empty string, got '%s'", value)
	}

	// But Exists should return true
	exists, err := cache.Exists(ctx, "emptyvalue")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected key with empty value to exist")
	}
}
