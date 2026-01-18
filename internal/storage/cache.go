// Package storage provides storage implementations including caching
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
)

const (
	// Default TTL for queue URL cache entries (24 hours)
	// Queue URLs rarely change, so a long TTL is appropriate
	defaultQueueURLTTL = 24 * time.Hour
)

// RedisCache implements the contracts.Cache interface using Redis
type RedisCache struct {
	client *redis.Client
	prefix string
}

// NewRedisCache creates a new Redis-based cache
func NewRedisCache(client *redis.Client, prefix string) *RedisCache {
	return &RedisCache{
		client: client,
		prefix: prefix,
	}
}

// buildKey constructs the full cache key with prefix
func (c *RedisCache) buildKey(key string) string {
	if c.prefix == "" {
		return key
	}
	return c.prefix + ":" + key
}

// Get retrieves a value from the cache
func (c *RedisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := c.client.Get(ctx, c.buildKey(key)).Result()
	if err == redis.Nil {
		return "", nil // Key doesn't exist
	}
	if err != nil {
		return "", fmt.Errorf("redis get failed: %w", err)
	}
	return val, nil
}

// Set stores a value in the cache with optional TTL (0 means no expiration)
func (c *RedisCache) Set(ctx context.Context, key string, value string, ttlSeconds int) error {
	var ttl time.Duration
	if ttlSeconds > 0 {
		ttl = time.Duration(ttlSeconds) * time.Second
	} else {
		ttl = defaultQueueURLTTL // Use default TTL for queue URLs
	}

	err := c.client.Set(ctx, c.buildKey(key), value, ttl).Err()
	if err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}
	return nil
}

// Delete removes a value from the cache
func (c *RedisCache) Delete(ctx context.Context, key string) error {
	err := c.client.Del(ctx, c.buildKey(key)).Err()
	if err != nil {
		return fmt.Errorf("redis delete failed: %w", err)
	}
	return nil
}

// DeleteByPrefix removes all values with the given prefix
func (c *RedisCache) DeleteByPrefix(ctx context.Context, prefix string) error {
	fullPrefix := c.buildKey(prefix)
	iter := c.client.Scan(ctx, 0, fullPrefix+"*", 0).Iterator()

	var keys []string
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("redis scan failed: %w", err)
	}

	if len(keys) > 0 {
		if err := c.client.Del(ctx, keys...).Err(); err != nil {
			return fmt.Errorf("redis delete by prefix failed: %w", err)
		}
	}

	return nil
}

// Exists checks if a key exists in the cache
func (c *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	count, err := c.client.Exists(ctx, c.buildKey(key)).Result()
	if err != nil {
		return false, fmt.Errorf("redis exists failed: %w", err)
	}
	return count > 0, nil
}

// Ping checks if the Redis connection is healthy
func (c *RedisCache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Ensure RedisCache implements contracts.Cache
var _ contracts.Cache = (*RedisCache)(nil)
