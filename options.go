package sqsmessaging

import (
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"gorm.io/gorm"

	"github.com/our-edu/go-sqs-messaging/internal/config"
)

// Options holds all configuration options for the client.
type Options struct {
	config             *config.Config
	serviceName        string
	logger             zerolog.Logger
	loggerSet          bool
	redisClient        *redis.Client
	db                 *gorm.DB
	prometheusRegistry prometheus.Registerer
}

// Option is a function that configures the client.
type Option func(*Options)

// WithAWSCredentials sets the AWS access key and secret key.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithAWSCredentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
//	)
func WithAWSCredentials(accessKeyID, secretAccessKey string) Option {
	return func(o *Options) {
		o.config.AWS.AccessKeyID = accessKeyID
		o.config.AWS.SecretAccessKey = secretAccessKey
	}
}

// WithAWSRegion sets the AWS region.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithAWSRegion("us-west-2"),
//	)
func WithAWSRegion(region string) Option {
	return func(o *Options) {
		o.config.AWS.Region = region
	}
}

// WithAWSEndpoint sets a custom AWS endpoint URL.
// This is useful for testing with LocalStack or other AWS-compatible services.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithAWSEndpoint("http://localhost:4566"),
//	)
func WithAWSEndpoint(endpoint string) Option {
	return func(o *Options) {
		o.config.AWS.Endpoint = endpoint
	}
}

// WithQueuePrefix sets the prefix for all queue names.
// This is typically used for environment isolation (e.g., "prod", "staging", "dev").
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithQueuePrefix("prod"),
//	)
//
// This will create queues like "prod-my-queue" instead of "my-queue".
func WithQueuePrefix(prefix string) Option {
	return func(o *Options) {
		o.config.SQS.Prefix = prefix
	}
}

// WithService sets the service name used in message envelopes.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithService("payment-service"),
//	)
func WithService(name string) Option {
	return func(o *Options) {
		o.serviceName = name
	}
}

// WithLogger sets a custom zerolog logger.
//
// Example:
//
//	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithLogger(logger),
//	)
func WithLogger(logger zerolog.Logger) Option {
	return func(o *Options) {
		o.logger = logger
		o.loggerSet = true
	}
}

// WithRedis configures Redis for idempotency checking.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithRedis("localhost:6379", "", 0),
//	)
func WithRedis(addr, password string, db int) Option {
	return func(o *Options) {
		o.redisClient = redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		})
		o.config.Redis.Host = addr
		o.config.Redis.Password = password
		o.config.Redis.DB = db
	}
}

// WithRedisClient sets a pre-configured Redis client.
//
// Example:
//
//	redisClient := redis.NewClient(&redis.Options{...})
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithRedisClient(redisClient),
//	)
func WithRedisClient(client *redis.Client) Option {
	return func(o *Options) {
		o.redisClient = client
	}
}

// WithDatabase sets the GORM database connection for idempotency persistence.
//
// Example:
//
//	db, _ := gorm.Open(mysql.Open(dsn), &gorm.Config{})
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithDatabase(db),
//	)
func WithDatabase(db *gorm.DB) Option {
	return func(o *Options) {
		o.db = db
	}
}

// WithVisibilityTimeout sets the SQS visibility timeout in seconds.
// Default is 30 seconds.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithVisibilityTimeout(60),
//	)
func WithVisibilityTimeout(seconds int) Option {
	return func(o *Options) {
		o.config.SQS.VisibilityTimeout = seconds
	}
}

// WithLongPollingWait sets the long polling wait time in seconds.
// Default is 20 seconds. Maximum is 20 seconds (SQS limitation).
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithLongPollingWait(20),
//	)
func WithLongPollingWait(seconds int) Option {
	return func(o *Options) {
		if seconds > 20 {
			seconds = 20
		}
		o.config.SQS.LongPollingWait = seconds
	}
}

// WithDLQMaxReceiveCount sets the number of receives before a message goes to DLQ.
// Default is 5.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithDLQMaxReceiveCount(3),
//	)
func WithDLQMaxReceiveCount(count int) Option {
	return func(o *Options) {
		o.config.SQS.DLQMaxReceiveCount = count
	}
}

// WithMessageRetention sets the message retention period in days.
// Default is 14 days. Maximum is 14 days.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithMessageRetention(7),
//	)
func WithMessageRetention(days int) Option {
	return func(o *Options) {
		if days > 14 {
			days = 14
		}
		o.config.SQS.MessageRetention = days
	}
}

// WithCloudWatchMetrics enables or disables CloudWatch metrics.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithCloudWatchMetrics(true, "MyApp/SQS"),
//	)
func WithCloudWatchMetrics(enabled bool, namespace string) Option {
	return func(o *Options) {
		o.config.SQS.CloudWatch.Enabled = enabled
		if namespace != "" {
			o.config.SQS.CloudWatch.Namespace = namespace
		}
	}
}

// WithPrometheusMetrics enables or disables Prometheus metrics.
// When enabled, use client.PrometheusHandler() to get the HTTP handler
// and mount it on your own HTTP server.
//
// Parameters:
//   - enabled: Whether to enable Prometheus metrics
//   - namespace: Prometheus metric namespace/prefix (e.g., "sqsmessaging")
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithPrometheusMetrics(true, "myapp_sqs"),
//	)
//
//	// Mount on your own HTTP server (e.g., Gin)
//	router.GET("/metrics", gin.WrapH(client.PrometheusHandler()))
//
//	// Or with net/http
//	http.Handle("/metrics", client.PrometheusHandler())
func WithPrometheusMetrics(enabled bool, namespace string) Option {
	return func(o *Options) {
		o.config.SQS.Prometheus.Enabled = enabled
		if namespace != "" {
			o.config.SQS.Prometheus.Namespace = namespace
		}
	}
}

// WithPrometheusRegistry sets a custom Prometheus registry for metrics registration.
// Use this when you have your own Prometheus registry and want SQS metrics
// to be registered there instead of the default global registry.
//
// This is useful when:
//   - You're using a custom registry for isolation
//   - You want to combine SQS metrics with your existing application metrics
//   - You need more control over metric registration
//
// Parameters:
//   - registry: A prometheus.Registerer (can be *prometheus.Registry or prometheus.DefaultRegisterer)
//
// Example with custom registry:
//
//	registry := prometheus.NewRegistry()
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithPrometheusMetrics(true, "myapp_sqs"),
//	    sqsmessaging.WithPrometheusRegistry(registry),
//	)
//
//	// Use promhttp.HandlerFor with your custom registry
//	router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))
//
// Example with default registry (explicitly):
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithPrometheusMetrics(true, "myapp_sqs"),
//	    sqsmessaging.WithPrometheusRegistry(prometheus.DefaultRegisterer),
//	)
//
//	// Use the standard promhttp.Handler()
//	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
func WithPrometheusRegistry(registry prometheus.Registerer) Option {
	return func(o *Options) {
		o.prometheusRegistry = registry
	}
}

// WithLongRunningEvents sets event types that need extended visibility timeout.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithLongRunningEvents("VideoProcessing", "ReportGeneration"),
//	)
func WithLongRunningEvents(eventTypes ...string) Option {
	return func(o *Options) {
		o.config.SQS.LongRunningEvents = eventTypes
	}
}

// WithEventTimeouts sets the visibility timeout mapping for specific event types.
// When a message with a matching event type is received, the visibility timeout
// will be changed to the specified value (in seconds) before processing begins.
// This allows different events to have different processing time allowances.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithEventTimeouts(map[string]int{
//	        "VideoProcessing":   600,  // 10 minutes
//	        "ReportGeneration":  300,  // 5 minutes
//	        "ImageResize":       120,  // 2 minutes
//	    }),
//	)
func WithEventTimeouts(timeouts map[string]int) Option {
	return func(o *Options) {
		if o.config.SQS.EventTimeouts == nil {
			o.config.SQS.EventTimeouts = make(map[string]int)
		}
		for eventType, timeout := range timeouts {
			o.config.SQS.EventTimeouts[eventType] = timeout
		}
	}
}

// WithTargetQueueMappings sets the event type to queue name mappings.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithTargetQueueMappings(map[string]string{
//	        "OrderCreated": "order-service-queue",
//	        "UserCreated":  "user-service-queue",
//	    }),
//	)
func WithTargetQueueMappings(mappings map[string]string) Option {
	return func(o *Options) {
		for k, v := range mappings {
			o.config.TargetQueue.Mappings[k] = v
		}
	}
}

// WithDefaultTargetQueue sets the default queue for unmapped event types.
//
// Example:
//
//	client, err := sqsmessaging.New(
//	    sqsmessaging.WithDefaultTargetQueue("default-queue"),
//	)
func WithDefaultTargetQueue(queueName string) Option {
	return func(o *Options) {
		o.config.TargetQueue.Default = queueName
	}
}

// ConsumerOption is a function that configures the consumer.
type ConsumerOption func(*consumerOptions)

type consumerOptions struct {
	maxMessages       int
	waitTime          int
	workerCount       int
	createIfNotExists bool
	onError           func(error)
	onMessageStart    func(Message)
	onMessageEnd      func(Message, error)
	errorBackoff      errorBackoffConfig
}

// errorBackoffConfig holds the configuration for exponential backoff on errors
type errorBackoffConfig struct {
	initialDelay   time.Duration
	maxDelay       time.Duration
	multiplier     float64
	consecutiveErr int // tracks consecutive errors for backoff calculation
}

// WithMaxMessages sets the maximum number of messages to receive per poll.
// Default is 10 (SQS maximum).
func WithMaxMessages(max int) ConsumerOption {
	return func(o *consumerOptions) {
		if max > 10 {
			max = 10
		}
		if max < 1 {
			max = 1
		}
		o.maxMessages = max
	}
}

// WithWaitTime sets the long polling wait time for the consumer.
// Default is 20 seconds.
func WithWaitTime(seconds int) ConsumerOption {
	return func(o *consumerOptions) {
		if seconds > 20 {
			seconds = 20
		}
		o.waitTime = seconds
	}
}

// WithOnError sets a callback for consumer errors.
func WithOnError(fn func(error)) ConsumerOption {
	return func(o *consumerOptions) {
		o.onError = fn
	}
}

// WithOnMessageStart sets a callback that's called before processing each message.
func WithOnMessageStart(fn func(Message)) ConsumerOption {
	return func(o *consumerOptions) {
		o.onMessageStart = fn
	}
}

// WithOnMessageEnd sets a callback that's called after processing each message.
func WithOnMessageEnd(fn func(Message, error)) ConsumerOption {
	return func(o *consumerOptions) {
		o.onMessageEnd = fn
	}
}

// WithWorkerCount sets the number of worker goroutines for multi-queue consumption.
// Default is one worker per queue. Increase this for higher throughput.
//
// Example:
//
//	client.StartMultiConsumer(ctx, queues,
//	    sqsmessaging.WithWorkerCount(10),
//	)
func WithWorkerCount(count int) ConsumerOption {
	return func(o *consumerOptions) {
		if count < 1 {
			count = 1
		}
		o.workerCount = count
	}
}

// WithCreateIfNotExists sets whether to create the queue if it doesn't exist.
// Default is true. Set to false if you want to fail when the queue doesn't exist.
//
// Example:
//
//	client.StartConsumer(ctx, "my-queue",
//	    sqsmessaging.WithCreateIfNotExists(false),
//	)
func WithCreateIfNotExists(create bool) ConsumerOption {
	return func(o *consumerOptions) {
		o.createIfNotExists = create
	}
}

// WithErrorBackoff configures exponential backoff for consumer errors.
// When receiving messages fails (e.g., queue doesn't exist, network issues),
// the consumer will wait with exponential backoff before retrying.
//
// Parameters:
//   - initialDelay: Starting delay after first error (default: 1 second)
//   - maxDelay: Maximum delay between retries (default: 30 seconds)
//   - multiplier: Factor to multiply delay by after each consecutive error (default: 2.0)
//
// Example:
//
//	client.StartConsumer(ctx, "my-queue",
//	    sqsmessaging.WithErrorBackoff(time.Second, 30*time.Second, 2.0),
//	)
func WithErrorBackoff(initialDelay, maxDelay time.Duration, multiplier float64) ConsumerOption {
	return func(o *consumerOptions) {
		if initialDelay > 0 {
			o.errorBackoff.initialDelay = initialDelay
		}
		if maxDelay > 0 {
			o.errorBackoff.maxDelay = maxDelay
		}
		if multiplier > 0 {
			o.errorBackoff.multiplier = multiplier
		}
	}
}
