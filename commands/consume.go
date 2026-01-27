package commands

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"

	"github.com/our-edu/go-sqs-messaging/pkg/config"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	sqsdriver "github.com/our-edu/go-sqs-messaging/internal/drivers/sqs"
	"github.com/our-edu/go-sqs-messaging/internal/messaging"
	"github.com/our-edu/go-sqs-messaging/internal/storage"
	"github.com/our-edu/go-sqs-messaging/pkg/envelope"
)

// Error types for classification
type ValidationError struct {
	msg string
}

func (e ValidationError) Error() string { return e.msg }

type TransientError struct {
	msg string
	err error
}

func (e TransientError) Error() string { return e.msg }
func (e TransientError) Unwrap() error { return e.err }

type PermanentError struct {
	msg string
}

func (e PermanentError) Error() string { return e.msg }

// runConsumer runs the SQS consumer
func runConsumer(ctx context.Context, cfg *config.Config, logger zerolog.Logger, queueName string, maxMessages, waitTime int) error {
	logger.Info().
		Str("queue", queueName).
		Int("max_messages", maxMessages).
		Int("wait_time", waitTime).
		Msg("Starting SQS consumer")

	// Initialize Redis client first (required for caching)
	redisClient, err := createRedisClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create Redis client: %w", err)
	}
	logger.Info().Msg("Redis connection verified")

	// Create Redis cache for queue URL resolution
	cache := createRedisCache(redisClient)

	// Initialize AWS SQS client
	sqsClient, err := createSQSClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	// Initialize components with Redis cache
	resolver := sqsdriver.NewResolver(sqsClient, cfg, logger, cache)
	consumer := sqsdriver.NewConsumer(sqsClient, resolver, cfg, logger)

	// Set up queue
	if err := consumer.SetQueue(ctx, queueName); err != nil {
		return fmt.Errorf("failed to set queue: %w", err)
	}

	// Initialize idempotency store
	idempotencyStore, err := createIdempotencyStore(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("failed to create idempotency store: %w", err)
	}

	// Initialize event registry
	registry := messaging.NewEventRegistry()
	// Register handlers from config (in production, this would be loaded from config)

	// Stats for error rate monitoring
	var totalProcessed, validationErrors, transientErrors, permanentErrors int

	// Main consume loop
	logger.Info().Msg("Consumer started, waiting for messages...")

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Consumer shutting down")
			return nil
		default:
		}

		// Receive messages
		messages, err := consumer.ReceiveMessages(ctx, maxMessages, waitTime)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to receive messages")
			continue
		}

		if len(messages) == 0 {
			continue
		}

		logger.Info().Int("count", len(messages)).Msg("Received messages")

		// Process each message
		for _, msg := range messages {
			totalProcessed++

			err := processMessage(ctx, consumer, idempotencyStore, registry, cfg, logger, msg)
			if err != nil {
				// Classify error
				var valErr ValidationError
				var transErr TransientError
				var permErr PermanentError

				switch {
				case errors.As(err, &valErr):
					validationErrors++
					logger.Warn().
						Str("message_id", msg.MessageID).
						Err(err).
						Msg("Validation error, deleting message")
					consumer.DeleteMessage(ctx, msg.ReceiptHandle)

				case errors.As(err, &transErr):
					transientErrors++
					logger.Warn().
						Str("message_id", msg.MessageID).
						Err(err).
						Msg("Transient error, leaving for retry")
					// Don't delete - will be retried or sent to DLQ

				case errors.As(err, &permErr):
					permanentErrors++
					logger.Error().
						Str("message_id", msg.MessageID).
						Err(err).
						Msg("Permanent error, deleting message")
					consumer.DeleteMessage(ctx, msg.ReceiptHandle)

				default:
					logger.Error().
						Str("message_id", msg.MessageID).
						Err(err).
						Msg("Unknown error")
				}
			} else {
				// Success - delete message
				if err := consumer.DeleteMessage(ctx, msg.ReceiptHandle); err != nil {
					logger.Error().
						Str("message_id", msg.MessageID).
						Err(err).
						Msg("Failed to delete message")
				}
			}
		}

		// Check error rates
		checkErrorRates(totalProcessed, validationErrors, transientErrors, logger)
	}
}

func processMessage(
	ctx context.Context,
	consumer *sqsdriver.Consumer,
	store *storage.IdempotencyStore,
	registry *messaging.EventRegistry,
	cfg *config.Config,
	logger zerolog.Logger,
	msg contracts.Message,
) error {
	// Parse envelope
	env, err := envelope.ParseEnvelopeFromMessage(msg.Body)
	if err != nil {
		return ValidationError{msg: fmt.Sprintf("invalid envelope: %v", err)}
	}

	// Validate envelope
	if err := env.Validate(); err != nil {
		return ValidationError{msg: fmt.Sprintf("envelope validation failed: %v", err)}
	}

	eventType := env.GetEventType()
	idempotencyKey := env.GetIdempotencyKey()

	logger.Debug().
		Str("event_type", eventType).
		Str("idempotency_key", idempotencyKey).
		Str("trace_id", env.GetTraceID()).
		Msg("Processing message")

	// Check idempotency
	processed, err := store.IsProcessed(ctx, idempotencyKey)
	if err != nil {
		return TransientError{msg: "idempotency check failed", err: err}
	}
	if processed {
		logger.Info().
			Str("idempotency_key", idempotencyKey).
			Msg("Message already processed, skipping")
		return nil
	}

	// Mark as processing
	if err := store.MarkProcessing(ctx, idempotencyKey); err != nil {
		logger.Info().
			Str("idempotency_key", idempotencyKey).
			Msg("Message is being processed by another consumer")
		return nil // Not an error, just skip
	}
	defer store.ClearProcessing(ctx, idempotencyKey)

	// Extend visibility for long-running events
	if cfg.IsLongRunningEvent(eventType) {
		consumer.ChangeVisibilityTimeout(ctx, msg.ReceiptHandle, 300) // 5 minutes
	}

	// Get handler
	handler, ok := registry.GetHandler(eventType)
	if !ok {
		return PermanentError{msg: fmt.Sprintf("no handler registered for event type: %s", eventType)}
	}

	// Execute handler
	startTime := time.Now()
	if err := handler(ctx, env.Payload); err != nil {
		// Classify the error
		if isTransientError(err) {
			return TransientError{msg: "handler failed", err: err}
		}
		return PermanentError{msg: fmt.Sprintf("handler failed: %v", err)}
	}
	duration := time.Since(startTime)

	// Mark as processed
	if err := store.MarkProcessed(ctx, idempotencyKey, eventType, env.Service); err != nil {
		logger.Warn().
			Str("idempotency_key", idempotencyKey).
			Err(err).
			Msg("Failed to mark as processed")
	}

	logger.Info().
		Str("event_type", eventType).
		Dur("duration", duration).
		Msg("Message processed successfully")

	return nil
}

func isTransientError(err error) bool {
	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Check for common transient error messages
	errMsg := strings.ToLower(err.Error())
	transientPatterns := []string{
		"connection",
		"timeout",
		"temporary",
		"unavailable",
		"retry",
		"throttl",
	}
	for _, pattern := range transientPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}
	return false
}

func checkErrorRates(total, validation, transient int, logger zerolog.Logger) {
	if total < 100 {
		return // Need enough samples
	}

	validationRate := float64(validation) / float64(total) * 100
	transientRate := float64(transient) / float64(total) * 100

	if validationRate > 1.0 {
		logger.Warn().
			Float64("rate_percent", validationRate).
			Msg("High validation error rate")
	}

	if transientRate > 10.0 {
		logger.Warn().
			Float64("rate_percent", transientRate).
			Msg("High transient error rate")
	}
}

func createRedisCache(redisClient *redis.Client) *storage.RedisCache {
	return storage.NewRedisCache(redisClient, "sqsmessaging")
}
