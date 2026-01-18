package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/cobra"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

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

// newConsumeCmd creates the consume command (equivalent to sqs:consume)
func newConsumeCmd() *cobra.Command {
	var maxMessages int
	var waitTime int

	cmd := &cobra.Command{
		Use:   "consume [queue]",
		Short: "Consume messages from an SQS queue",
		Long: `Consume messages from an SQS queue. This command is designed to be run
under Supervisor for production deployments.

The consumer implements:
- Long polling (default 20 seconds)
- Idempotency checking (Redis + Database)
- Error classification (validation, transient, permanent)
- Visibility timeout extension for long-running events
- CloudWatch metrics reporting`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			return runConsumer(cmd.Context(), queueName, maxMessages, waitTime)
		},
	}

	cmd.Flags().IntVarP(&maxMessages, "max", "m", 10, "Maximum messages to receive per poll")
	cmd.Flags().IntVarP(&waitTime, "wait", "w", 20, "Long polling wait time in seconds")

	return cmd
}

func runConsumer(ctx context.Context, queueName string, maxMessages, waitTime int) error {
	logger.Info().
		Str("queue", queueName).
		Int("max_messages", maxMessages).
		Int("wait_time", waitTime).
		Msg("Starting SQS consumer")

	// Initialize Redis client first (required for caching)
	redisClient, err := createRedisClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Redis client: %w", err)
	}
	logger.Info().Msg("Redis connection verified")

	// Create Redis cache for queue URL resolution
	cache := createRedisCache(redisClient)

	// Initialize AWS SQS client
	sqsClient, err := createSQSClient(ctx)
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
	idempotencyStore, err := createIdempotencyStore(ctx)
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

			err := processMessage(ctx, consumer, idempotencyStore, registry, msg)
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
		checkErrorRates(totalProcessed, validationErrors, transientErrors)
	}
}

func processMessage(
	ctx context.Context,
	consumer *sqsdriver.Consumer,
	store *storage.IdempotencyStore,
	registry *messaging.EventRegistry,
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

func checkErrorRates(total, validation, transient int) {
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

func createSQSClient(ctx context.Context) (*sqs.Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.AWS.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AWS.AccessKeyID,
			cfg.AWS.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(awsCfg), nil
}

// createRedisClient creates and validates a Redis client connection
func createRedisClient(ctx context.Context) (*redis.Client, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	// Test Redis connection
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return redisClient, nil
}

// createRedisCache creates a Redis cache for queue URL resolution
func createRedisCache(redisClient *redis.Client) *storage.RedisCache {
	return storage.NewRedisCache(redisClient, "sqsmessaging")
}

func createIdempotencyStore(ctx context.Context) (*storage.IdempotencyStore, error) {
	// Create Redis client
	redisClient, err := createRedisClient(ctx)
	if err != nil {
		return nil, err
	}

	// Create database connection
	var db *gorm.DB

	switch cfg.Database.Driver {
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			cfg.Database.Username,
			cfg.Database.Password,
			cfg.Database.Host,
			cfg.Database.Port,
			cfg.Database.Database,
		)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	case "postgres":
		dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			cfg.Database.Host,
			cfg.Database.Port,
			cfg.Database.Username,
			cfg.Database.Password,
			cfg.Database.Database,
		)
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", cfg.Database.Driver)
	}

	if err != nil {
		return nil, fmt.Errorf("database connection failed: %w", err)
	}

	store := storage.NewIdempotencyStore(redisClient, db, logger)

	// Auto-migrate the table
	if err := store.AutoMigrate(); err != nil {
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return store, nil
}
