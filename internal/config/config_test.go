package config

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Test messaging defaults
	if cfg.Messaging.Driver != DriverSQS {
		t.Errorf("expected driver '%s', got '%s'", DriverSQS, cfg.Messaging.Driver)
	}
	if cfg.Messaging.DualWrite != false {
		t.Error("expected DualWrite to be false")
	}
	if cfg.Messaging.FallbackToRabbitMQ != false {
		t.Error("expected FallbackToRabbitMQ to be false")
	}

	// Test SQS defaults
	if cfg.SQS.Prefix != "dev" {
		t.Errorf("expected prefix 'dev', got '%s'", cfg.SQS.Prefix)
	}
	if cfg.SQS.VisibilityTimeout != 30 {
		t.Errorf("expected VisibilityTimeout 30, got %d", cfg.SQS.VisibilityTimeout)
	}
	if cfg.SQS.LongPollingWait != 20 {
		t.Errorf("expected LongPollingWait 20, got %d", cfg.SQS.LongPollingWait)
	}
	if cfg.SQS.MessageRetention != 14 {
		t.Errorf("expected MessageRetention 14, got %d", cfg.SQS.MessageRetention)
	}
	if cfg.SQS.DLQMaxReceiveCount != 5 {
		t.Errorf("expected DLQMaxReceiveCount 5, got %d", cfg.SQS.DLQMaxReceiveCount)
	}

	// Test AWS defaults
	if cfg.AWS.Region != "us-east-2" {
		t.Errorf("expected Region 'us-east-2', got '%s'", cfg.AWS.Region)
	}

	// Test Redis defaults
	if cfg.Redis.Host != "localhost" {
		t.Errorf("expected Redis.Host 'localhost', got '%s'", cfg.Redis.Host)
	}
	if cfg.Redis.Port != 6379 {
		t.Errorf("expected Redis.Port 6379, got %d", cfg.Redis.Port)
	}

	// Test TargetQueue is initialized
	if cfg.TargetQueue.Mappings == nil {
		t.Error("expected TargetQueue.Mappings to be initialized")
	}
}

func TestGetPrefixedQueueName(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		queueName string
		expected  string
	}{
		{"with prefix", "prod", "my-queue", "prod-my-queue"},
		{"empty prefix", "", "my-queue", "my-queue"},
		{"dev prefix", "dev", "order-events", "dev-order-events"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.SQS.Prefix = tt.prefix

			result := cfg.GetPrefixedQueueName(tt.queueName)

			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestGetDLQName(t *testing.T) {
	tests := []struct {
		name      string
		queueName string
		expected  string
	}{
		{"simple queue", "my-queue", "my-queue-dlq"},
		{"prefixed queue", "prod-order-events", "prod-order-events-dlq"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()

			result := cfg.GetDLQName(tt.queueName)

			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestIsLongRunningEvent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SQS.LongRunningEvents = []string{"VideoProcessing", "ReportGeneration"}

	tests := []struct {
		name      string
		eventType string
		expected  bool
	}{
		{"long running event", "VideoProcessing", true},
		{"another long running", "ReportGeneration", true},
		{"normal event", "OrderCreated", false},
		{"empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cfg.IsLongRunningEvent(tt.eventType)

			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestGetTargetQueue(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TargetQueue.Default = "default-queue"
	cfg.TargetQueue.Mappings["OrderCreated"] = "order-queue"
	cfg.TargetQueue.Mappings["UserCreated"] = "user-queue"

	tests := []struct {
		name      string
		eventType string
		expected  string
	}{
		{"mapped event", "OrderCreated", "order-queue"},
		{"another mapped", "UserCreated", "user-queue"},
		{"unmapped event", "PaymentProcessed", "default-queue"},
		{"empty string", "", "default-queue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cfg.GetTargetQueue(tt.eventType)

			if result != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestGetVisibilityTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SQS.VisibilityTimeout = 60

	result := cfg.GetVisibilityTimeout()

	expected := 60 * time.Second
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestGetLongPollingWait(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SQS.LongPollingWait = 20

	result := cfg.GetLongPollingWait()

	expected := 20 * time.Second
	if result != expected {
		t.Errorf("expected %v, got %v", expected, result)
	}
}

func TestDriverTypeConstants(t *testing.T) {
	if DriverSQS != "sqs" {
		t.Errorf("expected DriverSQS to be 'sqs', got '%s'", DriverSQS)
	}
	if DriverRabbitMQ != "rabbitmq" {
		t.Errorf("expected DriverRabbitMQ to be 'rabbitmq', got '%s'", DriverRabbitMQ)
	}
}

func TestAWSConfigEndpoint(t *testing.T) {
	cfg := DefaultConfig()

	// Default should be empty
	if cfg.AWS.Endpoint != "" {
		t.Errorf("expected empty Endpoint by default, got '%s'", cfg.AWS.Endpoint)
	}

	// Can be set
	cfg.AWS.Endpoint = "http://localhost:4566"
	if cfg.AWS.Endpoint != "http://localhost:4566" {
		t.Errorf("expected Endpoint 'http://localhost:4566', got '%s'", cfg.AWS.Endpoint)
	}
}

func TestCloudWatchConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.SQS.CloudWatch.Enabled != true {
		t.Error("expected CloudWatch.Enabled to be true by default")
	}
	if cfg.SQS.CloudWatch.Namespace != "SQS/PaymentService" {
		t.Errorf("expected CloudWatch.Namespace 'SQS/PaymentService', got '%s'", cfg.SQS.CloudWatch.Namespace)
	}
}

func TestQueuesConfigInitialized(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Queues == nil {
		t.Error("expected Queues to be initialized")
	}
	if cfg.Events == nil {
		t.Error("expected Events to be initialized")
	}
}

func TestDatabaseConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Database.Driver != "mysql" {
		t.Errorf("expected Database.Driver 'mysql', got '%s'", cfg.Database.Driver)
	}
	if cfg.Database.Host != "localhost" {
		t.Errorf("expected Database.Host 'localhost', got '%s'", cfg.Database.Host)
	}
	if cfg.Database.Port != 3306 {
		t.Errorf("expected Database.Port 3306, got %d", cfg.Database.Port)
	}
}

func TestGetEventTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SQS.EventTimeouts = map[string]int{
		"VideoProcessing":  600,
		"ReportGeneration": 300,
		"ImageResize":      120,
	}

	tests := []struct {
		name           string
		eventType      string
		expectedTime   int
		expectedExists bool
	}{
		{"configured event - video", "VideoProcessing", 600, true},
		{"configured event - report", "ReportGeneration", 300, true},
		{"configured event - image", "ImageResize", 120, true},
		{"unconfigured event", "OrderCreated", 0, false},
		{"empty string", "", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeout, exists := cfg.GetEventTimeout(tt.eventType)

			if timeout != tt.expectedTime {
				t.Errorf("expected timeout %d, got %d", tt.expectedTime, timeout)
			}
			if exists != tt.expectedExists {
				t.Errorf("expected exists %v, got %v", tt.expectedExists, exists)
			}
		})
	}
}

func TestGetEventTimeout_NilMap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SQS.EventTimeouts = nil

	timeout, exists := cfg.GetEventTimeout("VideoProcessing")

	if timeout != 0 {
		t.Errorf("expected timeout 0 for nil map, got %d", timeout)
	}
	if exists {
		t.Error("expected exists to be false for nil map")
	}
}

func TestSetEventTimeout(t *testing.T) {
	cfg := DefaultConfig()

	// Set timeouts
	cfg.SetEventTimeout("VideoProcessing", 600)
	cfg.SetEventTimeout("ReportGeneration", 300)

	// Verify they were set
	if timeout, exists := cfg.GetEventTimeout("VideoProcessing"); !exists || timeout != 600 {
		t.Errorf("expected timeout 600, got %d (exists: %v)", timeout, exists)
	}
	if timeout, exists := cfg.GetEventTimeout("ReportGeneration"); !exists || timeout != 300 {
		t.Errorf("expected timeout 300, got %d (exists: %v)", timeout, exists)
	}
}

func TestSetEventTimeout_NilMap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SQS.EventTimeouts = nil

	// Should initialize map and set value
	cfg.SetEventTimeout("VideoProcessing", 600)

	if cfg.SQS.EventTimeouts == nil {
		t.Error("expected EventTimeouts map to be initialized")
	}
	if timeout, exists := cfg.GetEventTimeout("VideoProcessing"); !exists || timeout != 600 {
		t.Errorf("expected timeout 600, got %d (exists: %v)", timeout, exists)
	}
}

func TestSetEventTimeout_Override(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SetEventTimeout("VideoProcessing", 600)

	// Override with new value
	cfg.SetEventTimeout("VideoProcessing", 900)

	if timeout, exists := cfg.GetEventTimeout("VideoProcessing"); !exists || timeout != 900 {
		t.Errorf("expected timeout 900 after override, got %d (exists: %v)", timeout, exists)
	}
}

func TestEventTimeouts_Initialized(t *testing.T) {
	cfg := DefaultConfig()

	// EventTimeouts should be initialized (not nil)
	if cfg.SQS.EventTimeouts == nil {
		t.Error("expected EventTimeouts to be initialized")
	}
}
