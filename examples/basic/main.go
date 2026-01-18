// Package main demonstrates basic usage of the sqsmessaging library.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	sqsmessaging "github.com/our-edu/go-sqs-messaging"
)

func main() {
	// Create a new client with configuration options
	// Note: Redis is required for queue URL caching
	client, err := sqsmessaging.New(
		// AWS Configuration
		sqsmessaging.WithAWSRegion("us-east-2"),
		sqsmessaging.WithAWSCredentials(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
		),

		// Queue Configuration
		sqsmessaging.WithQueuePrefix("dev"),
		sqsmessaging.WithService("order-service"),

		// Redis Configuration (required for queue URL caching)
		sqsmessaging.WithRedis("localhost:6379", "", 0),

		// Optional: Database for idempotency persistence (uncomment if using)
		// sqsmessaging.WithDatabase(db), // GORM database instance
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Map event types to target queues
	client.SetTargetQueue("OrderCreated", "order-events")
	client.SetTargetQueue("OrderUpdated", "order-events")
	client.SetTargetQueue("PaymentProcessed", "payment-events")
	client.SetDefaultTargetQueue("default-events")

	// Register event handlers
	client.RegisterHandler("OrderCreated", handleOrderCreated)
	client.RegisterHandler("OrderUpdated", handleOrderUpdated)
	client.RegisterHandler("PaymentProcessed", handlePaymentProcessed)

	// Ensure queues exist (creates with DLQ)
	ctx := context.Background()
	if err := client.EnsureQueues(ctx, "order-events", "payment-events"); err != nil {
		log.Fatalf("Failed to ensure queues: %v", err)
	}

	// Example: Publish a message
	err = client.Publish(ctx, "OrderCreated", map[string]interface{}{
		"order_id":    "ORD-12345",
		"customer_id": "CUST-789",
		"total":       99.99,
		"items": []map[string]interface{}{
			{"sku": "PROD-001", "quantity": 2, "price": 49.99},
		},
	})
	if err != nil {
		log.Printf("Failed to publish: %v", err)
	}

	// Example: Batch publish
	results, err := client.PublishBatch(ctx, "order-events", []sqsmessaging.BatchMessage{
		{
			ID:        "msg-1",
			EventType: "OrderCreated",
			Payload:   map[string]interface{}{"order_id": "ORD-001"},
		},
		{
			ID:        "msg-2",
			EventType: "OrderCreated",
			Payload:   map[string]interface{}{"order_id": "ORD-002"},
		},
	})
	if err != nil {
		log.Printf("Batch publish failed: %v", err)
	} else {
		for _, r := range results {
			if r.Error != nil {
				log.Printf("Message %s failed: %v", r.ID, r.Error)
			} else {
				log.Printf("Message %s published with ID: %s", r.ID, r.MessageID)
			}
		}
	}

	// Start consumer in background
	consumerCtx, cancel := context.WithCancel(ctx)
	go func() {
		if err := client.StartConsumer(consumerCtx, "order-events",
			sqsmessaging.WithMaxMessages(10),
			sqsmessaging.WithWaitTime(20),
			sqsmessaging.WithOnError(func(err error) {
				log.Printf("Consumer error: %v", err)
			}),
		); err != nil && err != context.Canceled {
			log.Printf("Consumer stopped with error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
	cancel()
	time.Sleep(time.Second) // Give consumer time to finish
}

// Event Handlers

func handleOrderCreated(ctx context.Context, payload map[string]interface{}) error {
	orderID, _ := payload["order_id"].(string)
	customerID, _ := payload["customer_id"].(string)

	log.Printf("Processing OrderCreated: order=%s, customer=%s", orderID, customerID)

	// Simulate processing
	time.Sleep(100 * time.Millisecond)

	// Example: Return validation error (message will be deleted, not retried)
	if orderID == "" {
		return sqsmessaging.NewValidationError("order_id is required", nil)
	}

	// Example: Return transient error (message will be retried)
	// return sqsmessaging.NewTransientError("database temporarily unavailable", nil)

	// Example: Return permanent error (message will be deleted)
	// return sqsmessaging.NewPermanentError("order already processed", nil)

	return nil
}

func handleOrderUpdated(ctx context.Context, payload map[string]interface{}) error {
	orderID, _ := payload["order_id"].(string)
	status, _ := payload["status"].(string)

	log.Printf("Processing OrderUpdated: order=%s, status=%s", orderID, status)
	return nil
}

func handlePaymentProcessed(ctx context.Context, payload map[string]interface{}) error {
	paymentID, _ := payload["payment_id"].(string)
	amount, _ := payload["amount"].(float64)

	log.Printf("Processing PaymentProcessed: payment=%s, amount=%.2f", paymentID, amount)
	return nil
}

// DLQ Management Example
func dlqManagement(client *sqsmessaging.Client, queueName string) {
	ctx := context.Background()

	// Get DLQ depth
	depth, err := client.GetDLQDepth(ctx, queueName)
	if err != nil {
		log.Printf("Failed to get DLQ depth: %v", err)
		return
	}
	fmt.Printf("DLQ has %d messages\n", depth)

	// Inspect DLQ messages
	messages, err := client.InspectDLQ(ctx, queueName, 5)
	if err != nil {
		log.Printf("Failed to inspect DLQ: %v", err)
		return
	}
	for _, msg := range messages {
		fmt.Printf("DLQ Message: ID=%s, Body=%s\n", msg.MessageID, msg.Body[:100])
	}

	// Replay messages from DLQ back to main queue
	replayed, err := client.ReplayDLQ(ctx, queueName, 10)
	if err != nil {
		log.Printf("Failed to replay DLQ: %v", err)
		return
	}
	fmt.Printf("Replayed %d messages from DLQ\n", replayed)
}
