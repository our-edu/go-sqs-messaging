// Package storage provides idempotency storage implementations
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"gorm.io/gorm"
)

const (
	// Redis key prefixes
	keyProcessed  = "sqs:processed:"
	keyProcessing = "sqs:processing:"

	// TTL values
	processingTTL = 5 * time.Minute    // Lock expires after 5 minutes
	processedTTL  = 7 * 24 * time.Hour // Keep processed keys for 7 days
)

// ProcessedEvent represents a processed event record in the database
type ProcessedEvent struct {
	IdempotencyKey string    `gorm:"primaryKey;size:64"`
	EventType      string    `gorm:"size:100;not null"`
	Service        string    `gorm:"size:50;not null"`
	ProcessedAt    time.Time `gorm:"not null;index"`
}

// TableName returns the table name for ProcessedEvent
func (ProcessedEvent) TableName() string {
	return "processed_events"
}

// IdempotencyStore provides idempotency checking using Redis and database
type IdempotencyStore struct {
	redis  *redis.Client
	db     *gorm.DB
	logger zerolog.Logger
}

// NewIdempotencyStore creates a new idempotency store
func NewIdempotencyStore(redisClient *redis.Client, db *gorm.DB, logger zerolog.Logger) *IdempotencyStore {
	return &IdempotencyStore{
		redis:  redisClient,
		db:     db,
		logger: logger,
	}
}

// IsProcessed checks if an event has already been processed
// Uses Redis as fast path, database as slow path
func (s *IdempotencyStore) IsProcessed(ctx context.Context, idempotencyKey string) (bool, error) {
	// Fast path: Check Redis first
	exists, err := s.redis.Exists(ctx, keyProcessed+idempotencyKey).Result()
	if err != nil {
		s.logger.Warn().
			Str("key", idempotencyKey).
			Err(err).
			Msg("Redis check failed, falling back to database")
	} else if exists > 0 {
		return true, nil
	}

	// Slow path: Check database
	var count int64
	if err := s.db.WithContext(ctx).Model(&ProcessedEvent{}).
		Where("idempotency_key = ?", idempotencyKey).
		Count(&count).Error; err != nil {
		return false, fmt.Errorf("database check failed: %w", err)
	}

	if count > 0 {
		// Populate Redis cache for next time
		s.redis.Set(ctx, keyProcessed+idempotencyKey, "1", processedTTL)
		return true, nil
	}

	return false, nil
}

// MarkProcessing marks an event as currently being processed
// This prevents duplicate processing by concurrent consumers
func (s *IdempotencyStore) MarkProcessing(ctx context.Context, idempotencyKey string) error {
	// Use SetNX for atomic check-and-set
	set, err := s.redis.SetNX(ctx, keyProcessing+idempotencyKey, "1", processingTTL).Result()
	if err != nil {
		return fmt.Errorf("failed to set processing lock: %w", err)
	}
	if !set {
		return fmt.Errorf("event is already being processed")
	}
	return nil
}

// MarkProcessed marks an event as successfully processed
func (s *IdempotencyStore) MarkProcessed(ctx context.Context, idempotencyKey, eventType, service string) error {
	// Store in Redis
	if err := s.redis.Set(ctx, keyProcessed+idempotencyKey, "1", processedTTL).Err(); err != nil {
		s.logger.Warn().
			Str("key", idempotencyKey).
			Err(err).
			Msg("Failed to set Redis processed key")
	}

	// Store in database for durability
	event := ProcessedEvent{
		IdempotencyKey: idempotencyKey,
		EventType:      eventType,
		Service:        service,
		ProcessedAt:    time.Now(),
	}
	if err := s.db.WithContext(ctx).Create(&event).Error; err != nil {
		// Ignore duplicate key errors (already processed)
		s.logger.Debug().
			Str("key", idempotencyKey).
			Err(err).
			Msg("Event already in database")
	}

	// Clear processing lock
	s.redis.Del(ctx, keyProcessing+idempotencyKey)

	return nil
}

// ClearProcessing removes the processing lock
func (s *IdempotencyStore) ClearProcessing(ctx context.Context, idempotencyKey string) error {
	return s.redis.Del(ctx, keyProcessing+idempotencyKey).Err()
}

// Cleanup removes old processed event records from the database
func (s *IdempotencyStore) Cleanup(ctx context.Context, olderThanDays int) (int64, error) {
	cutoff := time.Now().AddDate(0, 0, -olderThanDays)
	result := s.db.WithContext(ctx).
		Where("processed_at < ?", cutoff).
		Delete(&ProcessedEvent{})

	if result.Error != nil {
		return 0, fmt.Errorf("cleanup failed: %w", result.Error)
	}

	s.logger.Info().
		Int64("deleted", result.RowsAffected).
		Int("older_than_days", olderThanDays).
		Msg("Cleaned up processed events")

	return result.RowsAffected, nil
}

// AutoMigrate creates or updates the processed_events table
func (s *IdempotencyStore) AutoMigrate() error {
	return s.db.AutoMigrate(&ProcessedEvent{})
}
