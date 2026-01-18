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
        orderID := payload["order_id"].(string)
        log.Printf("Processing order: %s", orderID)
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

## Consumer Options

| Option | Description |
|--------|-------------|
| `WithMaxMessages(n)` | Messages per poll (max 10) |
| `WithWaitTime(seconds)` | Long polling wait (max 20) |
| `WithWorkerCount(n)` | Worker goroutines |
| `WithOnError(fn)` | Error callback |
| `WithCreateIfNotExists(bool)` | Auto-create queues |

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

## Batch Publishing

```go
results, err := client.PublishBatch(ctx, "order-events", []sqsmessaging.BatchMessage{
    {ID: "1", EventType: "OrderCreated", Payload: payload1},
    {ID: "2", EventType: "OrderCreated", Payload: payload2},
})
```

## Environment Variables

```bash
AWS_SQS_ACCESS_KEY_ID=your-key
AWS_SQS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=us-east-2
AWS_ENDPOINT=http://localhost:4566  # Optional, for LocalStack
SQS_QUEUE_PREFIX=prod
```
