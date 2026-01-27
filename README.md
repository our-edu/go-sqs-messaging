# go-sqs-messaging

A Go library for AWS SQS messaging with automatic queue creation, dead letter queues, and idempotency support.

## Installation

```bash
go get github.com/our-edu/go-sqs-messaging
```

## Quick Start

### Publishing Messages

```go
package main

import (
    "context"
    "log"

    sqsmessaging "github.com/our-edu/go-sqs-messaging"
)

func main() {
    client, err := sqsmessaging.New(
        sqsmessaging.WithAWSRegion("us-east-2"),
        sqsmessaging.WithQueuePrefix("prod"),
        sqsmessaging.WithService("order-service"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Map event types to queues
    client.SetTargetQueue("OrderCreated", "order-events")

    // Publish
    ctx := context.Background()
    err = client.Publish(ctx, "OrderCreated", map[string]any{
        "order_id": "12345",
        "amount":   99.99,
    })
}
```

### Consuming Messages

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    sqsmessaging "github.com/our-edu/go-sqs-messaging"
)

func main() {
    client, err := sqsmessaging.New(
        sqsmessaging.WithAWSRegion("us-east-2"),
        sqsmessaging.WithQueuePrefix("prod"),
        sqsmessaging.WithService("order-service"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // Register handlers
    client.RegisterHandler("OrderCreated", func(ctx context.Context, payload map[string]any) error {
        // Access message metadata from context
        sourceService := sqsmessaging.SourceServiceFromContext(ctx)
        queueName := sqsmessaging.QueueNameFromContext(ctx)
        traceID := sqsmessaging.TraceIDFromContext(ctx)
        
        orderID := payload["order_id"].(string)
        log.Printf("Processing order %s from service %s (queue: %s, trace: %s)", 
            orderID, sourceService, queueName, traceID)
        return nil
    })

    // Start consuming
    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        client.StartConsumer(ctx, "order-events")
    }()

    // Graceful shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh
    cancel()
}
```

### Multi-Queue Consumer

```go
client.RegisterHandler("UserCreated", handleUser)
client.RegisterHandler("OrderCreated", handleOrder)

queues := []string{"user-events", "order-events", "payment-events"}
client.StartMultiConsumer(ctx, queues,
    sqsmessaging.WithWorkerCount(3),
)
```

## Configuration Options

| Option | Description |
|--------|-------------|
| `WithAWSRegion(region)` | AWS region |
| `WithAWSCredentials(key, secret)` | AWS credentials |
| `WithAWSEndpoint(url)` | Custom endpoint (LocalStack) |
| `WithQueuePrefix(prefix)` | Queue name prefix (e.g., "prod", "dev") |
| `WithService(name)` | Service name for message envelope |
| `WithVisibilityTimeout(seconds)` | Message visibility timeout |
| `WithDLQMaxReceiveCount(count)` | Retries before DLQ |
| `WithRedis(addr, password, db)` | Redis for idempotency |
| `WithDatabase(db)` | GORM DB for idempotency |
| `WithCloudWatchMetrics(enabled, namespace)` | CloudWatch metrics |
| `WithPrometheusMetrics(enabled, namespace)` | Prometheus metrics |
| `WithLongPollingWait(seconds)` | Long polling wait time (max 20s) |
| `WithMessageRetention(days)` | Message retention period (max 14 days) |
| `WithEventTimeouts(map)` | Per-event visibility timeout |

## Message Context Helpers

When processing messages, your handler receives a context that contains useful metadata about the message. Use these helper functions to access it:

```go
func handleOrder(ctx context.Context, payload map[string]any) error {
    // Get the service that published this message
    sourceService := sqsmessaging.SourceServiceFromContext(ctx)
    
    // Get the queue name the message was received from
    queueName := sqsmessaging.QueueNameFromContext(ctx)
    
    // Get the trace ID for distributed tracing
    traceID := sqsmessaging.TraceIDFromContext(ctx)
    
    // Get the SQS message ID
    messageID := sqsmessaging.MessageIDFromContext(ctx)
    
    // Get the event type
    eventType := sqsmessaging.EventTypeFromContext(ctx)
    
    log.Printf("Received %s from %s via queue %s", eventType, sourceService, queueName)
    
    // Handle differently based on source service
    switch sourceService {
    case "order-service":
        // Handle order service events
    case "payment-service":
        // Handle payment service events
    }
    
    return nil
}
```

| Helper Function | Returns |
|-----------------|---------|
| `SourceServiceFromContext(ctx)` | Service that published the message (e.g., `"order-service"`) |
| `QueueNameFromContext(ctx)` | Queue name the message was received from |
| `EventTypeFromContext(ctx)` | Event type (e.g., `"OrderCreated"`) |
| `TraceIDFromContext(ctx)` | Trace ID for distributed tracing |
| `MessageIDFromContext(ctx)` | SQS Message ID |

## Consumer Options

| Option | Description |
|--------|-------------|
| `WithMaxMessages(n)` | Messages per poll (max 10) |
| `WithWaitTime(seconds)` | Long polling wait (max 20) |
| `WithWorkerCount(n)` | Worker goroutines |
| `WithOnError(fn)` | Error callback |
| `WithCreateIfNotExists(bool)` | Auto-create queues |
| `WithErrorBackoff(initial, max, multiplier)` | Exponential backoff on errors |
| `WithOnMessageStart(fn)` | Callback before message processing |
| `WithOnMessageEnd(fn)` | Callback after message processing |

## Error Handling

Return typed errors to control retry behavior:

```go
// Validation error - message deleted, no retry
return sqsmessaging.NewValidationError("invalid order_id", nil)

// Transient error - message retried
return sqsmessaging.NewTransientError("database unavailable", nil)

// Permanent error - message deleted
return sqsmessaging.NewPermanentError("order already processed", nil)
```

## LocalStack Testing

```go
client, _ := sqsmessaging.New(
    sqsmessaging.WithAWSEndpoint("http://localhost:4566"),
    sqsmessaging.WithAWSRegion("us-east-1"),
    sqsmessaging.WithAWSCredentials("test", "test"),
    sqsmessaging.WithQueuePrefix("local"),
)
```

## DLQ Management

```go
// Check DLQ depth
depth, _ := client.GetDLQDepth(ctx, "order-events")

// Inspect DLQ messages
messages, _ := client.InspectDLQ(ctx, "order-events", 10)

// Replay messages back to main queue
replayed, _ := client.ReplayDLQ(ctx, "order-events", 10)
```

## CLI Command Extension

If you're building a CLI application with Cobra and want to include SQS messaging management commands, you can extend your CLI with the built-in commands:

```go
package main

import (
    "context"
    "os"
    "os/signal"
    "syscall"

    "github.com/rs/zerolog"
    "github.com/spf13/cobra"

    "github.com/our-edu/go-sqs-messaging/commands"
    "github.com/our-edu/go-sqs-messaging/pkg/config"
)

func main() {
    logger := zerolog.New(os.Stderr).With().Timestamp().Logger()
    cfg := config.Load()

    rootCmd := &cobra.Command{
        Use:   "myapp",
        Short: "My Application CLI",
    }

    // Add your own commands
    rootCmd.AddCommand(&cobra.Command{
        Use:   "version",
        Short: "Show version",
        Run: func(cmd *cobra.Command, args []string) {
            cmd.Println("MyApp v1.0.0")
        },
    })

    // Add SQS messaging commands
    commands.AddCommands(rootCmd, cfg, logger)

    // Execute
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigCh
        logger.Info().Msg("Received shutdown signal")
        cancel()
    }()

    if err := rootCmd.ExecuteContext(ctx); err != nil {
        logger.Error().Err(err).Msg("Command failed")
        os.Exit(1)
    }
}
```

This will add commands like `consume`, `ensure`, `status`, `cleanup`, `inspect-dlq`, `monitor-dlq`, `replay-dlq`, `test-connection`, and `test-receive` to your CLI.

## Batch Publishing

```go
results, err := client.PublishBatch(ctx, "order-events", []sqsmessaging.BatchMessage{
    {ID: "1", EventType: "OrderCreated", Payload: payload1},
    {ID: "2", EventType: "OrderCreated", Payload: payload2},
})
```

## Monitoring & Metrics

### Prometheus Metrics

The library exposes Prometheus metrics that can be integrated with your application's existing metrics setup.

#### Enable Prometheus Metrics (Default Registry)

By default, metrics are registered with Prometheus's default registry:

```go
client, err := sqsmessaging.New(
    sqsmessaging.WithPrometheusMetrics(true, "sqsmessaging"),
    // ... other options
)

// Use the standard promhttp.Handler() - SQS metrics will be included automatically
router.GET("/metrics", gin.WrapH(promhttp.Handler()))
```

#### Using a Custom Registry

If you're using a custom Prometheus registry, you have two options:

**Option 1: Pass your registry to the library**

```go
registry := prometheus.NewRegistry()

// Register your own metrics
registry.MustRegister(myCustomCounter)

client, err := sqsmessaging.New(
    sqsmessaging.WithPrometheusMetrics(true, "sqsmessaging"),
    sqsmessaging.WithPrometheusRegistry(registry),  // Pass your custom registry
    // ... other options
)

// Use promhttp.HandlerFor with your custom registry
router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))
```

**Option 2: Manually register collectors**

```go
registry := prometheus.NewRegistry()

// Register standard Go metrics
registry.MustRegister(collectors.NewGoCollector())
registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

// Register your own application metrics
registry.MustRegister(myCustomCounter)

client, err := sqsmessaging.New(
    sqsmessaging.WithPrometheusMetrics(true, "sqsmessaging"),
    // ... other options
)

// Register SQS messaging metrics to your custom registry
for _, collector := range client.PrometheusCollectors() {
    registry.MustRegister(collector)
}

// Use promhttp.HandlerFor with your custom registry
router.GET("/metrics", gin.WrapH(promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))
```

#### Available Metrics

**Counters:**
- `sqsmessaging_messages_processed_total` - Total messages processed
- `sqsmessaging_messages_success_total` - Total successful messages
- `sqsmessaging_validation_errors_total` - Validation errors
- `sqsmessaging_transient_errors_total` - Transient errors (retried)
- `sqsmessaging_permanent_errors_total` - Permanent errors
- `sqsmessaging_messages_published_total` - Messages published
- `sqsmessaging_publish_errors_total` - Publish errors

**Gauges:**
- `sqsmessaging_queue_depth` - Current messages in queue
- `sqsmessaging_dlq_depth` - Current messages in DLQ
- `sqsmessaging_active_consumers` - Active consumer count

**Histograms:**
- `sqsmessaging_processing_duration_milliseconds` - Message processing time
- `sqsmessaging_publish_duration_milliseconds` - Publish latency

All metrics are labeled with `queue` and/or `event_type` for detailed observability.

#### Example Prometheus Query

```promql
# Message throughput (per second)
rate(sqsmessaging_messages_processed_total[1m])

# Error rate
rate(sqsmessaging_permanent_errors_total[1m])

# Queue depth trend
sqsmessaging_queue_depth

# P99 processing latency
histogram_quantile(0.99, sqsmessaging_processing_duration_milliseconds_bucket)
```

### CloudWatch Metrics

The library also supports CloudWatch metrics (enabled by default):

```go
client, err := sqsmessaging.New(
    sqsmessaging.WithCloudWatchMetrics(true, "MyApp/SQS"),
    // ... other options
)
```

## Resilience & Error Recovery

### Consumer Error Backoff

When a consumer encounters errors (queue doesn't exist, network issues, etc.), it automatically applies exponential backoff to avoid overwhelming the system. This is especially useful when dealing with service restarts or LocalStack failures.

```go
client.StartConsumer(ctx, "order-events",
    sqsmessaging.WithErrorBackoff(
        1*time.Second,    // Initial delay
        30*time.Second,   // Max delay
        2.0,              // Multiplier
    ),
)
```

**Default behavior (no configuration needed):**
- Initial delay: 1 second
- Max delay: 30 seconds
- Multiplier: 2.0x per consecutive error
- Automatic queue recreation for non-existent queues

**Example backoff sequence:**
- 1st error: wait 1s
- 2nd error: wait 2s
- 3rd error: wait 4s
- 4th error: wait 8s
- ... continues up to 30s max

This prevents the consumer from spamming error logs when the queue is temporarily unavailable.

## Environment Variables

```bash
AWS_SQS_ACCESS_KEY_ID=your-key
AWS_SQS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=us-east-2
AWS_ENDPOINT=http://localhost:4566  # Optional, for LocalStack
SQS_QUEUE_PREFIX=prod

# Prometheus metrics
SQS_PROMETHEUS_ENABLED=true
SQS_PROMETHEUS_NAMESPACE=sqsmessaging
SQS_PROMETHEUS_SUBSYSTEM=

# CloudWatch metrics
SQS_CLOUDWATCH_ENABLED=true
SQS_CLOUDWATCH_NAMESPACE=MyApp/SQS
```
