// Package config provides configuration management for the SQS messaging system.
// It mirrors the Laravel PHP configuration structure for easy migration.
package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

// DriverType represents the messaging driver type
type DriverType string

const (
	DriverSQS      DriverType = "sqs"
	DriverRabbitMQ DriverType = "rabbitmq"
)

// Config holds all configuration for the messaging system
type Config struct {
	Messaging   MessagingConfig
	SQS         SQSConfig
	Queues      QueuesConfig
	Events      EventsConfig
	TargetQueue TargetQueueConfig
	AWS         AWSConfig
	Redis       RedisConfig
	Database    DatabaseConfig
}

// MessagingConfig controls driver selection and migration modes
type MessagingConfig struct {
	// Driver is the active messaging driver (sqs or rabbitmq)
	Driver DriverType `json:"driver" yaml:"driver"`
	// DualWrite sends messages to both SQS and RabbitMQ simultaneously
	DualWrite bool `json:"dual_write" yaml:"dual_write"`
	// FallbackToRabbitMQ falls back to RabbitMQ if SQS fails
	FallbackToRabbitMQ bool `json:"fallback_to_rabbitmq" yaml:"fallback_to_rabbitmq"`
}

// SQSConfig holds AWS SQS-specific settings
type SQSConfig struct {
	// Prefix for queue names (usually environment like dev, staging, prod)
	Prefix string `json:"prefix" yaml:"prefix"`
	// AutoEnsure automatically creates queues on startup
	AutoEnsure bool `json:"auto_ensure" yaml:"auto_ensure"`
	// LongRunningEvents is a list of event types that need extended visibility timeout
	LongRunningEvents []string `json:"long_running_events" yaml:"long_running_events"`
	// VisibilityTimeout is the default visibility timeout in seconds
	VisibilityTimeout int `json:"visibility_timeout" yaml:"visibility_timeout"`
	// LongPollingWait is the wait time for long polling in seconds
	LongPollingWait int `json:"long_polling_wait" yaml:"long_polling_wait"`
	// MessageRetention is the message retention period in days
	MessageRetention int `json:"message_retention" yaml:"message_retention"`
	// DLQMaxReceiveCount is the number of receives before moving to DLQ
	DLQMaxReceiveCount int `json:"dlq_max_receive_count" yaml:"dlq_max_receive_count"`
	// CloudWatch settings
	CloudWatch CloudWatchConfig `json:"cloudwatch" yaml:"cloudwatch"`
}

// CloudWatchConfig holds CloudWatch metrics settings
type CloudWatchConfig struct {
	// Enabled enables CloudWatch metrics
	Enabled bool `json:"enabled" yaml:"enabled"`
	// Namespace is the CloudWatch namespace
	Namespace string `json:"namespace" yaml:"namespace"`
}

// AWSConfig holds AWS credentials and region
type AWSConfig struct {
	AccessKeyID     string `json:"access_key_id" yaml:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key" yaml:"secret_access_key"`
	Region          string `json:"region" yaml:"region"`
}

// RedisConfig holds Redis connection settings
type RedisConfig struct {
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Password string `json:"password" yaml:"password"`
	DB       int    `json:"db" yaml:"db"`
}

// DatabaseConfig holds database connection settings
type DatabaseConfig struct {
	Driver   string `json:"driver" yaml:"driver"` // mysql, postgres
	Host     string `json:"host" yaml:"host"`
	Port     int    `json:"port" yaml:"port"`
	Database string `json:"database" yaml:"database"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

// ServiceQueues defines queues owned by a service
type ServiceQueues struct {
	Default  string   `json:"default" yaml:"default"`
	Specific []string `json:"specific" yaml:"specific"`
}

// QueuesConfig maps service names to their queue definitions
type QueuesConfig map[string]ServiceQueues

// EventHandler represents a handler function for an event type
type EventHandler func(payload map[string]interface{}) error

// EventsConfig maps event types to their handlers
type EventsConfig map[string]EventHandler

// TargetQueueConfig maps event types to target consumer queues
type TargetQueueConfig struct {
	Mappings map[string]string `json:"mappings" yaml:"mappings"`
	Default  string            `json:"default" yaml:"default"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Messaging: MessagingConfig{
			Driver:             DriverSQS,
			DualWrite:          false,
			FallbackToRabbitMQ: false,
		},
		SQS: SQSConfig{
			Prefix:             "dev",
			AutoEnsure:         false,
			LongRunningEvents:  []string{},
			VisibilityTimeout:  30,
			LongPollingWait:    20,
			MessageRetention:   14,
			DLQMaxReceiveCount: 5,
			CloudWatch: CloudWatchConfig{
				Enabled:   true,
				Namespace: "SQS/PaymentService",
			},
		},
		AWS: AWSConfig{
			AccessKeyID:     "",
			SecretAccessKey: "",
			Region:          "us-east-2",
		},
		Redis: RedisConfig{
			Host:     "localhost",
			Port:     6379,
			Password: "",
			DB:       0,
		},
		Database: DatabaseConfig{
			Driver:   "mysql",
			Host:     "localhost",
			Port:     3306,
			Database: "messaging",
			Username: "root",
			Password: "",
		},
		Queues: make(QueuesConfig),
		Events: make(EventsConfig),
		TargetQueue: TargetQueueConfig{
			Mappings: make(map[string]string),
			Default:  "admission-service-queue",
		},
	}
}

// GetPrefixedQueueName returns the queue name with environment prefix
func (c *Config) GetPrefixedQueueName(queueName string) string {
	if c.SQS.Prefix == "" {
		return queueName
	}
	return c.SQS.Prefix + "-" + queueName
}

// GetDLQName returns the Dead Letter Queue name for a given queue
func (c *Config) GetDLQName(queueName string) string {
	return queueName + "-dlq"
}

// IsLongRunningEvent checks if an event type requires extended visibility timeout
func (c *Config) IsLongRunningEvent(eventType string) bool {
	for _, e := range c.SQS.LongRunningEvents {
		if e == eventType {
			return true
		}
	}
	return false
}

// GetTargetQueue returns the target queue for an event type
func (c *Config) GetTargetQueue(eventType string) string {
	if queue, ok := c.TargetQueue.Mappings[eventType]; ok {
		return queue
	}
	return c.TargetQueue.Default
}

// GetVisibilityTimeout returns the visibility timeout as a duration
func (c *Config) GetVisibilityTimeout() time.Duration {
	return time.Duration(c.SQS.VisibilityTimeout) * time.Second
}

// GetLongPollingWait returns the long polling wait time as a duration
func (c *Config) GetLongPollingWait() time.Duration {
	return time.Duration(c.SQS.LongPollingWait) * time.Second
}

// Helper functions using Viper

func getViperString(key, defaultValue string) string {
	if viper.IsSet(key) {
		return viper.GetString(key)
	}
	return defaultValue
}

func getViperBool(key string, defaultValue bool) bool {
	if viper.IsSet(key) {
		return viper.GetBool(key)
	}
	return defaultValue
}

func getViperInt(key string, defaultValue int) int {
	if viper.IsSet(key) {
		return viper.GetInt(key)
	}
	return defaultValue
}

// LoadDotEnv loads environment variables from .env file using Viper
func LoadDotEnv() error {
	viper.SetConfigName(".env")
	viper.SetConfigType("env")
	viper.AddConfigPath(".")
	viper.AddConfigPath("../")
	viper.AddConfigPath("../../")

	// Automatically read environment variables
	viper.AutomaticEnv()

	// Read .env file - it's okay if it doesn't exist
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}
	return nil
}

// Load loads configuration from .env file and environment variables
func Load() *Config {
	// Load .env file if it exists (ignoring errors if not found)
	_ = LoadDotEnv()

	// Now load from viper
	return LoadFromViper()
}

// LoadFromViper loads configuration from Viper (after .env is loaded)
func LoadFromViper() *Config {
	cfg := DefaultConfig()

	// Messaging config
	if driver := getViperString("MESSAGING_DRIVER", ""); driver != "" {
		cfg.Messaging.Driver = DriverType(driver)
	}
	cfg.Messaging.DualWrite = getViperBool("MESSAGING_DUAL_WRITE", false)
	cfg.Messaging.FallbackToRabbitMQ = getViperBool("MESSAGING_FALLBACK_TO_RABBITMQ", false)

	// AWS config
	cfg.AWS.AccessKeyID = getViperString("AWS_SQS_ACCESS_KEY_ID", cfg.AWS.AccessKeyID)
	cfg.AWS.SecretAccessKey = getViperString("AWS_SQS_SECRET_ACCESS_KEY", cfg.AWS.SecretAccessKey)
	cfg.AWS.Region = getViperString("AWS_DEFAULT_REGION", cfg.AWS.Region)

	// SQS config
	if prefix := getViperString("SQS_QUEUE_PREFIX", ""); prefix != "" {
		cfg.SQS.Prefix = prefix
	}
	cfg.SQS.AutoEnsure = getViperBool("SQS_AUTO_ENSURE", false)
	cfg.SQS.VisibilityTimeout = getViperInt("SQS_VISIBILITY_TIMEOUT", cfg.SQS.VisibilityTimeout)
	cfg.SQS.LongPollingWait = getViperInt("SQS_LONG_POLLING_WAIT", cfg.SQS.LongPollingWait)
	cfg.SQS.MessageRetention = getViperInt("SQS_MESSAGE_RETENTION", cfg.SQS.MessageRetention)
	cfg.SQS.DLQMaxReceiveCount = getViperInt("SQS_DLQ_MAX_RECEIVE_COUNT", cfg.SQS.DLQMaxReceiveCount)

	// Long running events (comma-separated)
	if events := getViperString("SQS_LONG_RUNNING_EVENTS", ""); events != "" {
		cfg.SQS.LongRunningEvents = strings.Split(events, ",")
		for i := range cfg.SQS.LongRunningEvents {
			cfg.SQS.LongRunningEvents[i] = strings.TrimSpace(cfg.SQS.LongRunningEvents[i])
		}
	}

	// CloudWatch
	cfg.SQS.CloudWatch.Enabled = getViperBool("SQS_CLOUDWATCH_ENABLED", true)
	if ns := getViperString("SQS_CLOUDWATCH_NAMESPACE", ""); ns != "" {
		cfg.SQS.CloudWatch.Namespace = ns
	}

	// Target queue config
	if defaultQueue := getViperString("SQS_DEFAULT_TARGET_QUEUE", ""); defaultQueue != "" {
		cfg.TargetQueue.Default = defaultQueue
	}

	// Target queue mappings (format: EVENT:queue,EVENT2:queue2)
	if mappings := getViperString("SQS_TARGET_QUEUE_MAPPINGS", ""); mappings != "" {
		for _, mapping := range strings.Split(mappings, ",") {
			parts := strings.SplitN(strings.TrimSpace(mapping), ":", 2)
			if len(parts) == 2 {
				cfg.TargetQueue.Mappings[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	// Redis config
	cfg.Redis.Host = getViperString("REDIS_HOST", cfg.Redis.Host)
	cfg.Redis.Port = getViperInt("REDIS_PORT", cfg.Redis.Port)
	cfg.Redis.Password = getViperString("REDIS_PASSWORD", cfg.Redis.Password)
	cfg.Redis.DB = getViperInt("REDIS_DB", cfg.Redis.DB)

	// Database config
	cfg.Database.Driver = getViperString("DB_CONNECTION", cfg.Database.Driver)
	cfg.Database.Host = getViperString("DB_HOST", cfg.Database.Host)
	cfg.Database.Port = getViperInt("DB_PORT", cfg.Database.Port)
	cfg.Database.Database = getViperString("DB_DATABASE", cfg.Database.Database)
	cfg.Database.Username = getViperString("DB_USERNAME", cfg.Database.Username)
	cfg.Database.Password = getViperString("DB_PASSWORD", cfg.Database.Password)

	return cfg
}
