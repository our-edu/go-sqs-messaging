package sqsmessaging

import (
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/our-edu/go-sqs-messaging/internal/config"
)

func TestWithAWSCredentials(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithAWSCredentials("test-access-key", "test-secret-key")(opts)

	if opts.config.AWS.AccessKeyID != "test-access-key" {
		t.Errorf("expected AccessKeyID 'test-access-key', got '%s'", opts.config.AWS.AccessKeyID)
	}
	if opts.config.AWS.SecretAccessKey != "test-secret-key" {
		t.Errorf("expected SecretAccessKey 'test-secret-key', got '%s'", opts.config.AWS.SecretAccessKey)
	}
}

func TestWithAWSRegion(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithAWSRegion("eu-west-1")(opts)

	if opts.config.AWS.Region != "eu-west-1" {
		t.Errorf("expected Region 'eu-west-1', got '%s'", opts.config.AWS.Region)
	}
}

func TestWithAWSEndpoint(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithAWSEndpoint("http://localhost:4566")(opts)

	if opts.config.AWS.Endpoint != "http://localhost:4566" {
		t.Errorf("expected Endpoint 'http://localhost:4566', got '%s'", opts.config.AWS.Endpoint)
	}
}

func TestWithQueuePrefix(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithQueuePrefix("prod")(opts)

	if opts.config.SQS.Prefix != "prod" {
		t.Errorf("expected Prefix 'prod', got '%s'", opts.config.SQS.Prefix)
	}
}

func TestWithService(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithService("payment-service")(opts)

	if opts.serviceName != "payment-service" {
		t.Errorf("expected serviceName 'payment-service', got '%s'", opts.serviceName)
	}
}

func TestWithVisibilityTimeout(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithVisibilityTimeout(60)(opts)

	if opts.config.SQS.VisibilityTimeout != 60 {
		t.Errorf("expected VisibilityTimeout 60, got %d", opts.config.SQS.VisibilityTimeout)
	}
}

func TestWithLongPollingWait(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"normal value", 10, 10},
		{"max value", 20, 20},
		{"over max", 30, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &Options{config: defaultTestConfig()}
			WithLongPollingWait(tt.input)(opts)

			if opts.config.SQS.LongPollingWait != tt.expected {
				t.Errorf("expected LongPollingWait %d, got %d", tt.expected, opts.config.SQS.LongPollingWait)
			}
		})
	}
}

func TestWithDLQMaxReceiveCount(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithDLQMaxReceiveCount(3)(opts)

	if opts.config.SQS.DLQMaxReceiveCount != 3 {
		t.Errorf("expected DLQMaxReceiveCount 3, got %d", opts.config.SQS.DLQMaxReceiveCount)
	}
}

func TestWithMessageRetention(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"normal value", 7, 7},
		{"max value", 14, 14},
		{"over max", 21, 14},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &Options{config: defaultTestConfig()}
			WithMessageRetention(tt.input)(opts)

			if opts.config.SQS.MessageRetention != tt.expected {
				t.Errorf("expected MessageRetention %d, got %d", tt.expected, opts.config.SQS.MessageRetention)
			}
		})
	}
}

func TestWithCloudWatchMetrics(t *testing.T) {
	tests := []struct {
		name              string
		enabled           bool
		namespace         string
		expectedEnabled   bool
		expectedNamespace string
	}{
		{"enabled with namespace", true, "MyApp/SQS", true, "MyApp/SQS"},
		{"disabled", false, "", false, "SQS/PaymentService"}, // default namespace preserved
		{"enabled empty namespace", true, "", true, "SQS/PaymentService"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &Options{config: defaultTestConfig()}
			WithCloudWatchMetrics(tt.enabled, tt.namespace)(opts)

			if opts.config.SQS.CloudWatch.Enabled != tt.expectedEnabled {
				t.Errorf("expected Enabled %v, got %v", tt.expectedEnabled, opts.config.SQS.CloudWatch.Enabled)
			}
			if opts.config.SQS.CloudWatch.Namespace != tt.expectedNamespace {
				t.Errorf("expected Namespace '%s', got '%s'", tt.expectedNamespace, opts.config.SQS.CloudWatch.Namespace)
			}
		})
	}
}

func TestWithLongRunningEvents(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithLongRunningEvents("VideoProcessing", "ReportGeneration")(opts)

	if len(opts.config.SQS.LongRunningEvents) != 2 {
		t.Errorf("expected 2 long running events, got %d", len(opts.config.SQS.LongRunningEvents))
	}
	if opts.config.SQS.LongRunningEvents[0] != "VideoProcessing" {
		t.Errorf("expected first event 'VideoProcessing', got '%s'", opts.config.SQS.LongRunningEvents[0])
	}
	if opts.config.SQS.LongRunningEvents[1] != "ReportGeneration" {
		t.Errorf("expected second event 'ReportGeneration', got '%s'", opts.config.SQS.LongRunningEvents[1])
	}
}

func TestWithTargetQueueMappings(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	mappings := map[string]string{
		"OrderCreated": "order-service-queue",
		"UserCreated":  "user-service-queue",
	}
	WithTargetQueueMappings(mappings)(opts)

	if opts.config.TargetQueue.Mappings["OrderCreated"] != "order-service-queue" {
		t.Errorf("expected mapping for OrderCreated, got '%s'", opts.config.TargetQueue.Mappings["OrderCreated"])
	}
	if opts.config.TargetQueue.Mappings["UserCreated"] != "user-service-queue" {
		t.Errorf("expected mapping for UserCreated, got '%s'", opts.config.TargetQueue.Mappings["UserCreated"])
	}
}

func TestWithDefaultTargetQueue(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithDefaultTargetQueue("default-queue")(opts)

	if opts.config.TargetQueue.Default != "default-queue" {
		t.Errorf("expected Default 'default-queue', got '%s'", opts.config.TargetQueue.Default)
	}
}

func TestWithRedis(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}

	WithRedis("localhost:6379", "password", 1)(opts)

	if opts.redisClient == nil {
		t.Error("expected redisClient to be set")
	}
	if opts.config.Redis.Host != "localhost:6379" {
		t.Errorf("expected Redis.Host 'localhost:6379', got '%s'", opts.config.Redis.Host)
	}
	if opts.config.Redis.Password != "password" {
		t.Errorf("expected Redis.Password 'password', got '%s'", opts.config.Redis.Password)
	}
	if opts.config.Redis.DB != 1 {
		t.Errorf("expected Redis.DB 1, got %d", opts.config.Redis.DB)
	}
}

func TestWithRedisClient(t *testing.T) {
	opts := &Options{config: defaultTestConfig()}
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	WithRedisClient(redisClient)(opts)

	if opts.redisClient != redisClient {
		t.Error("expected redisClient to match provided client")
	}
}

// Consumer Options Tests

func TestWithMaxMessages(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"normal value", 5, 5},
		{"max value", 10, 10},
		{"over max", 15, 10},
		{"zero", 0, 1},
		{"negative", -1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &consumerOptions{}
			WithMaxMessages(tt.input)(opts)

			if opts.maxMessages != tt.expected {
				t.Errorf("expected maxMessages %d, got %d", tt.expected, opts.maxMessages)
			}
		})
	}
}

func TestWithWaitTime(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"normal value", 10, 10},
		{"max value", 20, 20},
		{"over max", 30, 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &consumerOptions{}
			WithWaitTime(tt.input)(opts)

			if opts.waitTime != tt.expected {
				t.Errorf("expected waitTime %d, got %d", tt.expected, opts.waitTime)
			}
		})
	}
}

func TestWithWorkerCount(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		{"normal value", 5, 5},
		{"one", 1, 1},
		{"zero", 0, 1},
		{"negative", -1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &consumerOptions{}
			WithWorkerCount(tt.input)(opts)

			if opts.workerCount != tt.expected {
				t.Errorf("expected workerCount %d, got %d", tt.expected, opts.workerCount)
			}
		})
	}
}

func TestWithCreateIfNotExists(t *testing.T) {
	tests := []struct {
		name     string
		input    bool
		expected bool
	}{
		{"true", true, true},
		{"false", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &consumerOptions{}
			WithCreateIfNotExists(tt.input)(opts)

			if opts.createIfNotExists != tt.expected {
				t.Errorf("expected createIfNotExists %v, got %v", tt.expected, opts.createIfNotExists)
			}
		})
	}
}

func TestWithOnError(t *testing.T) {
	opts := &consumerOptions{}
	called := false
	fn := func(err error) { called = true }

	WithOnError(fn)(opts)

	if opts.onError == nil {
		t.Error("expected onError to be set")
	}
	opts.onError(nil)
	if !called {
		t.Error("expected callback to be called")
	}
}

func TestWithOnMessageStart(t *testing.T) {
	opts := &consumerOptions{}
	called := false
	fn := func(msg Message) { called = true }

	WithOnMessageStart(fn)(opts)

	if opts.onMessageStart == nil {
		t.Error("expected onMessageStart to be set")
	}
	opts.onMessageStart(Message{})
	if !called {
		t.Error("expected callback to be called")
	}
}

func TestWithOnMessageEnd(t *testing.T) {
	opts := &consumerOptions{}
	called := false
	fn := func(msg Message, err error) { called = true }

	WithOnMessageEnd(fn)(opts)

	if opts.onMessageEnd == nil {
		t.Error("expected onMessageEnd to be set")
	}
	opts.onMessageEnd(Message{}, nil)
	if !called {
		t.Error("expected callback to be called")
	}
}

// Helper to create default test config
func defaultTestConfig() *config.Config {
	return config.DefaultConfig()
}
