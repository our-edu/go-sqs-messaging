package sqsmessaging

import (
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"gorm.io/gorm"

	"github.com/our-edu/go-sqs-messaging/internal/config"
)

// Options holds all configuration options for the client.
type Options struct {
	config      *config.Config
	serviceName string
	logger      zerolog.Logger
	loggerSet   bool
	redisClient *redis.Client
	db          *gorm.DB
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
