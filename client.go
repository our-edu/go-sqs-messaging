// Package sqsmessaging provides a robust AWS SQS messaging library for Go applications.
//
// This package is a Go port of the Laravel our-edu/laravel-sqs-messaging package,
// providing the same features including:
//   - AWS SQS integration with automatic queue/DLQ creation
//   - Message envelope with idempotency keys
//   - Redis-based queue URL caching (required)
//   - Redis + Database idempotency checking
//   - Error classification (validation, transient, permanent)
//   - CloudWatch metrics integration
//   - Dead Letter Queue (DLQ) management
//   - RabbitMQ fallback support for migration scenarios
//
// Redis is a required dependency for queue URL caching and must be configured.
//
// Basic Usage:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithAWSRegion("us-east-2"),
//	    sqsmessaging.WithQueuePrefix("prod"),
//	    sqsmessaging.WithService("payment-service"),
//	    sqsmessaging.WithRedis("localhost:6379", "", 0),  // Required
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//
//	// Publish a message
//	err = client.Publish(ctx, "UserCreated", map[string]any{
//	    "user_id": 123,
//	    "email":   "user@example.com",
//	})
//
//	// Consume messages
//	client.RegisterHandler("UserCreated", func(ctx context.Context, payload map[string]any) error {
//	    // Handle the event
//	    return nil
//	})
//	client.StartConsumer(ctx, "my-queue")
package sqsmessaging

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"gorm.io/gorm"

	"github.com/our-edu/go-sqs-messaging/internal/config"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	sqsdriver "github.com/our-edu/go-sqs-messaging/internal/drivers/sqs"
	"github.com/our-edu/go-sqs-messaging/internal/messaging"
	"github.com/our-edu/go-sqs-messaging/internal/metrics"
	"github.com/our-edu/go-sqs-messaging/internal/storage"
)

// Client is the main entry point for the SQS messaging library.
// It provides methods for publishing messages, consuming messages,
// and managing queues.
type Client struct {
	config           *config.Config
	sqsClient        *sqs.Client
	cloudwatchClient *cloudwatch.Client
	redisClient      *redis.Client
	cache            *storage.RedisCache
	db               *gorm.DB
	logger           zerolog.Logger

	resolver          *sqsdriver.Resolver
	targetResolver    *sqsdriver.TargetQueueResolver
	publisher         *sqsdriver.Publisher
	consumer          *sqsdriver.Consumer
	metricsService    *metrics.CloudWatchService
	prometheusService *metrics.PrometheusService
	idempotencyStore  *storage.IdempotencyStore
	messagingService  *messaging.Service
	eventRegistry     *messaging.EventRegistry

	serviceName string
	mu          sync.RWMutex
	closed      bool
}

// New creates a new SQS messaging client with the provided options.
// Redis is required for queue URL caching and must be configured using
// WithRedis() or WithRedisClient() options.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithAWSCredentials("access-key", "secret-key"),
//	    sqsmessaging.WithAWSRegion("us-east-2"),
//	    sqsmessaging.WithQueuePrefix("prod"),
//	    sqsmessaging.WithService("my-service"),
//	    sqsmessaging.WithRedis("localhost:6379", "", 0),  // Required
//	)
func New(opts ...Option) (*Client, error) {
	// Apply default configuration
	cfg := config.DefaultConfig()
	options := &Options{
		config: cfg,
	}

	// Apply user options
	for _, opt := range opts {
		opt(options)
	}

	// Validate required options - Redis is now mandatory
	if options.redisClient == nil {
		return nil, ErrRedisRequired
	}

	if options.serviceName == "" {
		options.serviceName = "default-service"
	}

	// Initialize logger - check if a logger was explicitly set
	logger := options.logger
	if !options.loggerSet {
		logger = zerolog.New(os.Stderr).With().Timestamp().Logger()
	}

	// Validate Redis connection
	ctx := context.Background()
	if err := options.redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRedisConnectionFailed, err)
	}
	logger.Info().Msg("Redis connection verified")

	// Create AWS config
	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.AWS.Region),
	}

	if cfg.AWS.AccessKeyID != "" && cfg.AWS.SecretAccessKey != "" {
		awsOpts = append(awsOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AWS.AccessKeyID,
				cfg.AWS.SecretAccessKey,
				"",
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create AWS clients with optional custom endpoint (for LocalStack, etc.)
	var sqsClient *sqs.Client
	var cloudwatchClient *cloudwatch.Client

	if cfg.AWS.Endpoint != "" {
		sqsClient = sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
			o.BaseEndpoint = &cfg.AWS.Endpoint
		})
		cloudwatchClient = cloudwatch.NewFromConfig(awsCfg, func(o *cloudwatch.Options) {
			o.BaseEndpoint = &cfg.AWS.Endpoint
		})
	} else {
		sqsClient = sqs.NewFromConfig(awsCfg)
		cloudwatchClient = cloudwatch.NewFromConfig(awsCfg)
	}

	// Create Redis cache for queue URL resolution
	redisCache := storage.NewRedisCache(options.redisClient, "sqsmessaging")

	// Create client
	client := &Client{
		config:           cfg,
		sqsClient:        sqsClient,
		cloudwatchClient: cloudwatchClient,
		redisClient:      options.redisClient,
		cache:            redisCache,
		db:               options.db,
		logger:           logger,
		serviceName:      options.serviceName,
	}

	// Initialize components with Redis cache
	client.resolver = sqsdriver.NewResolver(sqsClient, cfg, logger, redisCache)
	client.targetResolver = sqsdriver.NewTargetQueueResolver(cfg)
	client.publisher = sqsdriver.NewPublisher(sqsClient, client.resolver, cfg, logger, options.serviceName)
	client.consumer = sqsdriver.NewConsumer(sqsClient, client.resolver, cfg, logger)
	client.metricsService = metrics.NewCloudWatchService(cloudwatchClient, cfg, logger)
	client.eventRegistry = messaging.NewEventRegistry()
	client.messagingService = messaging.NewService(cfg, logger)

	// Initialize Prometheus metrics if enabled
	if cfg.SQS.Prometheus.Enabled {
		promConfig := metrics.PrometheusConfig{
			Namespace: cfg.SQS.Prometheus.Namespace,
			Subsystem: cfg.SQS.Prometheus.Subsystem,
		}
		client.prometheusService = metrics.NewPrometheusService(logger, promConfig)
		if err := client.prometheusService.Register(); err != nil {
			logger.Warn().Err(err).Msg("Failed to register Prometheus metrics")
		}
		logger.Info().Msg("Prometheus metrics enabled - use client.PrometheusHandler() to expose metrics endpoint")
	}

	// Initialize idempotency store if DB is provided (Redis is already verified)
	if client.db != nil {
		client.idempotencyStore = storage.NewIdempotencyStore(client.redisClient, client.db, logger)
		if err := client.idempotencyStore.AutoMigrate(); err != nil {
			logger.Warn().Err(err).Msg("Failed to auto-migrate idempotency table")
		}
	}

	// Register SQS driver
	adapter := sqsdriver.NewPublisherAdapter(client.publisher, client.targetResolver, logger)
	sqsDriverImpl := sqsdriver.NewDriver(adapter, client.resolver, cfg, logger)
	client.messagingService.RegisterDriver(config.DriverSQS, sqsDriverImpl)
	client.messagingService.RegisterEventRegistry(client.eventRegistry)

	return client, nil
}

// Close releases all resources held by the client.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	if c.redisClient != nil {
		c.redisClient.Close()
	}

	return nil
}

// Publish sends a message to the appropriate queue based on event type mapping.
//
// Example:
//
//	err := client.Publish(ctx, "OrderCreated", map[string]any{
//	    "order_id": "12345",
//	    "amount":   99.99,
//	})
func (c *Client) Publish(ctx context.Context, eventType string, payload map[string]any) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return ErrClientClosed
	}

	return c.messagingService.PublishRaw(ctx, eventType, payload)
}

// PublishToQueue sends a message directly to a specific queue.
//
// Example:
//
//	msgID, err := client.PublishToQueue(ctx, "my-queue", "OrderCreated", payload)
func (c *Client) PublishToQueue(ctx context.Context, queueName, eventType string, payload map[string]any) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return "", ErrClientClosed
	}

	return c.publisher.Publish(ctx, queueName, eventType, payload)
}

// PublishBatch sends multiple messages to a queue in a single batch.
// Maximum 10 messages per batch (SQS limitation).
//
// Example:
//
//	results, err := client.PublishBatch(ctx, "my-queue", []sqsmessaging.BatchMessage{
//	    {ID: "1", EventType: "OrderCreated", Payload: payload1},
//	    {ID: "2", EventType: "OrderCreated", Payload: payload2},
//	})
func (c *Client) PublishBatch(ctx context.Context, queueName string, messages []BatchMessage) ([]BatchResult, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, ErrClientClosed
	}

	// Convert to internal contracts type
	contractMsgs := make([]contracts.BatchMessage, len(messages))
	for i, m := range messages {
		contractMsgs[i] = contracts.BatchMessage{
			ID:        m.ID,
			EventType: m.EventType,
			Payload:   m.Payload,
		}
	}

	results, err := c.publisher.PublishBatch(ctx, queueName, contractMsgs)
	if err != nil {
		return nil, err
	}

	// Convert results
	out := make([]BatchResult, len(results))
	for i, r := range results {
		out[i] = BatchResult{
			ID:        r.ID,
			MessageID: r.MessageID,
			Error:     r.Error,
		}
	}
	return out, nil
}

// RegisterHandler registers a handler function for an event type.
// The handler will be called when a message of that type is consumed.
//
// Example:
//
//	client.RegisterHandler("OrderCreated", func(ctx context.Context, payload map[string]any) error {
//	    orderID := payload["order_id"].(string)
//	    // Process the order...
//	    return nil
//	})
func (c *Client) RegisterHandler(eventType string, handler Handler) {
	// Handler and contracts.EventHandler have the same signature, so we can cast directly
	c.eventRegistry.Register(eventType, contracts.EventHandler(handler))
}

// StartConsumer starts consuming messages from the specified queue.
// This is a blocking call that runs until the context is cancelled.
// The queue will be automatically created with a DLQ if it doesn't exist.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	// Register handlers first
//	client.RegisterHandler("OrderCreated", orderHandler)
//
//	// Start consuming (blocks until context is cancelled)
//	err := client.StartConsumer(ctx, "order-queue")
func (c *Client) StartConsumer(ctx context.Context, queueName string, opts ...ConsumerOption) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	c.mu.RUnlock()

	// Apply consumer options
	consumerOpts := &consumerOptions{
		maxMessages:       10,
		waitTime:          20,
		createIfNotExists: true, // Default: create queue if not exists
		errorBackoff: errorBackoffConfig{
			initialDelay: 1 * time.Second,
			maxDelay:     30 * time.Second,
			multiplier:   2.0,
		},
	}
	for _, opt := range opts {
		opt(consumerOpts)
	}

	// Ensure queue exists (creates with DLQ if not exists)
	if consumerOpts.createIfNotExists {
		c.logger.Info().
			Str("queue", queueName).
			Msg("Ensuring queue exists")
		if _, err := c.resolver.CreateQueueWithDLQ(ctx, queueName); err != nil {
			return fmt.Errorf("failed to ensure queue exists: %w", err)
		}
	}

	// Set up queue for consumption
	if err := c.consumer.SetQueue(ctx, queueName); err != nil {
		return fmt.Errorf("failed to set queue: %w", err)
	}

	c.logger.Info().
		Str("queue", queueName).
		Int("max_messages", consumerOpts.maxMessages).
		Int("wait_time", consumerOpts.waitTime).
		Msg("Starting consumer")

	return c.runConsumerLoop(ctx, queueName, consumerOpts)
}

// StartMultiConsumer starts consuming messages from multiple queues concurrently.
// This is useful for services like notification service that need to consume
// from multiple queues (e.g., user-notifications, order-notifications, payment-notifications).
// All queues will be automatically created with DLQs if they don't exist.
//
// Example:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	// Register handlers first
//	client.RegisterHandler("UserNotification", userNotificationHandler)
//	client.RegisterHandler("OrderNotification", orderNotificationHandler)
//	client.RegisterHandler("PaymentNotification", paymentNotificationHandler)
//
//	// Start consuming from multiple queues
//	err := client.StartMultiConsumer(ctx,
//	    []string{"user-notifications", "order-notifications", "payment-notifications"},
//	    sqsmessaging.WithWorkerCount(5), // Optional: set worker pool size
//	)
func (c *Client) StartMultiConsumer(ctx context.Context, queueNames []string, opts ...ConsumerOption) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClientClosed
	}
	c.mu.RUnlock()

	if len(queueNames) == 0 {
		return fmt.Errorf("no queues specified")
	}

	// Apply consumer options
	consumerOpts := &consumerOptions{
		maxMessages:       10,
		waitTime:          20,
		workerCount:       len(queueNames), // Default: one worker per queue
		createIfNotExists: true,
		errorBackoff: errorBackoffConfig{
			initialDelay: 1 * time.Second,
			maxDelay:     30 * time.Second,
			multiplier:   2.0,
		},
	}
	for _, opt := range opts {
		opt(consumerOpts)
	}

	// Ensure all queues exist
	if consumerOpts.createIfNotExists {
		for _, queueName := range queueNames {
			c.logger.Info().
				Str("queue", queueName).
				Msg("Ensuring queue exists")
			if _, err := c.resolver.CreateQueueWithDLQ(ctx, queueName); err != nil {
				return fmt.Errorf("failed to ensure queue %s exists: %w", queueName, err)
			}
		}
	}

	c.logger.Info().
		Strs("queues", queueNames).
		Int("max_messages", consumerOpts.maxMessages).
		Int("wait_time", consumerOpts.waitTime).
		Int("worker_count", consumerOpts.workerCount).
		Msg("Starting multi-queue consumer")

	return c.runMultiConsumerLoop(ctx, queueNames, consumerOpts)
}

// EnsureQueues creates all queues defined in the target queue mappings.
// Each queue is created with an associated Dead Letter Queue.
func (c *Client) EnsureQueues(ctx context.Context, queueNames ...string) error {
	for _, name := range queueNames {
		_, err := c.resolver.CreateQueueWithDLQ(ctx, name)
		if err != nil {
			return fmt.Errorf("failed to ensure queue %s: %w", name, err)
		}
	}
	return nil
}

// GetQueueDepth returns the approximate number of messages in a queue.
func (c *Client) GetQueueDepth(ctx context.Context, queueName string) (int64, error) {
	if err := c.consumer.SetQueue(ctx, queueName); err != nil {
		return 0, err
	}
	return c.consumer.GetQueueDepth(ctx)
}

// GetDLQDepth returns the approximate number of messages in a queue's DLQ.
func (c *Client) GetDLQDepth(ctx context.Context, queueName string) (int64, error) {
	return c.consumer.GetDLQDepth(ctx, queueName)
}

// InspectDLQ retrieves messages from the Dead Letter Queue for inspection.
func (c *Client) InspectDLQ(ctx context.Context, queueName string, limit int) ([]Message, error) {
	msgs, err := c.consumer.ReceiveFromDLQ(ctx, queueName, limit)
	if err != nil {
		return nil, err
	}

	result := make([]Message, len(msgs))
	for i, m := range msgs {
		result[i] = Message{
			MessageID:     m.MessageID,
			ReceiptHandle: m.ReceiptHandle,
			Body:          m.Body,
			Attributes:    m.Attributes,
		}
	}
	return result, nil
}

// ReplayDLQ moves messages from the DLQ back to the main queue.
func (c *Client) ReplayDLQ(ctx context.Context, queueName string, limit int) (int, error) {
	msgs, err := c.consumer.ReceiveFromDLQ(ctx, queueName, limit)
	if err != nil {
		return 0, err
	}

	replayed := 0
	for _, msg := range msgs {
		if err := c.consumer.ReplayFromDLQ(ctx, queueName, msg); err != nil {
			c.logger.Error().
				Str("message_id", msg.MessageID).
				Err(err).
				Msg("Failed to replay message")
		} else {
			replayed++
		}
	}
	return replayed, nil
}

// CleanupProcessedEvents removes old idempotency records from the database.
func (c *Client) CleanupProcessedEvents(ctx context.Context, olderThanDays int) (int64, error) {
	if c.idempotencyStore == nil {
		return 0, ErrIdempotencyNotConfigured
	}
	return c.idempotencyStore.Cleanup(ctx, olderThanDays)
}

// PrometheusHandler returns the HTTP handler for Prometheus metrics.
// This allows you to mount the metrics endpoint on your own HTTP server,
// giving you full control over the HTTP configuration, middleware, and port.
// Returns nil if Prometheus metrics are not enabled.
//
// Example with net/http:
//
//	http.Handle("/metrics", client.PrometheusHandler())
//	http.ListenAndServe(":8080", nil)
//
// Example with Gin:
//
//	router := gin.Default()
//	router.GET("/metrics", gin.WrapH(client.PrometheusHandler()))
//
// Example with Echo:
//
//	e := echo.New()
//	e.GET("/metrics", echo.WrapHandler(client.PrometheusHandler()))
//
// Example with Chi:
//
//	r := chi.NewRouter()
//	r.Handle("/metrics", client.PrometheusHandler())
func (c *Client) PrometheusHandler() http.Handler {
	if c.prometheusService == nil {
		return nil
	}
	return c.prometheusService.Handler()
}

// PrometheusEnabled returns true if Prometheus metrics are enabled.
func (c *Client) PrometheusEnabled() bool {
	return c.prometheusService != nil
}

// SetTargetQueue maps an event type to a target queue.
//
// Example:
//
//	client.SetTargetQueue("OrderCreated", "order-service-queue")
func (c *Client) SetTargetQueue(eventType, queueName string) {
	c.config.TargetQueue.Mappings[eventType] = queueName
}

// SetDefaultTargetQueue sets the default queue for unmapped event types.
func (c *Client) SetDefaultTargetQueue(queueName string) {
	c.config.TargetQueue.Default = queueName
}
