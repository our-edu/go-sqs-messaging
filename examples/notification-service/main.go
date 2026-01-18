// Package main demonstrates a notification service that consumes from multiple queues.
// This is a common pattern where a notification service needs to listen to events
// from multiple services (user, order, payment) and send notifications accordingly.
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
	// Create a new client for the notification service
	client, err := sqsmessaging.New(
		// AWS Configuration
		sqsmessaging.WithAWSRegion("us-east-2"),

		// LocalStack Configuration for local development
		// sqsmessaging.WithAWSEndpoint("http://localhost:4566"),
		sqsmessaging.WithAWSCredentials("test", "test"),

		// Queue Configuration
		sqsmessaging.WithQueuePrefix("dev"),
		sqsmessaging.WithService("notification-service"),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Register handlers for different notification types from various services
	// User service notifications
	client.RegisterHandler("UserRegistered", handleUserRegistered)
	client.RegisterHandler("UserPasswordReset", handleUserPasswordReset)
	client.RegisterHandler("UserEmailVerified", handleUserEmailVerified)

	// Order service notifications
	client.RegisterHandler("OrderCreated", handleOrderCreated)
	client.RegisterHandler("OrderShipped", handleOrderShipped)
	client.RegisterHandler("OrderDelivered", handleOrderDelivered)

	// Payment service notifications
	client.RegisterHandler("PaymentSuccessful", handlePaymentSuccessful)
	client.RegisterHandler("PaymentFailed", handlePaymentFailed)
	client.RegisterHandler("RefundProcessed", handleRefundProcessed)

	// Define the queues that this notification service will consume from
	// Each queue corresponds to a different source service
	queues := []string{
		"user-notifications",    // Events from user service
		"order-notifications",   // Events from order service
		"payment-notifications", // Events from payment service
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Start consuming from multiple queues concurrently
	// The StartMultiConsumer will:
	// 1. Ensure all queues exist (create with DLQ if not)
	// 2. Start a goroutine for each queue to poll for messages
	// 3. Process messages using the registered handlers
	go func() {
		log.Printf("Starting notification service consuming from queues: %v", queues)
		if err := client.StartMultiConsumer(ctx, queues,
			sqsmessaging.WithMaxMessages(10),
			sqsmessaging.WithWaitTime(20),
			sqsmessaging.WithWorkerCount(len(queues)), // One worker per queue
			sqsmessaging.WithOnError(func(err error) {
				log.Printf("Consumer error: %v", err)
			}),
			sqsmessaging.WithOnMessageStart(func(msg sqsmessaging.Message) {
				log.Printf("Started processing message: %s", msg.MessageID)
			}),
			sqsmessaging.WithOnMessageEnd(func(msg sqsmessaging.Message, err error) {
				if err != nil {
					log.Printf("Failed to process message %s: %v", msg.MessageID, err)
				} else {
					log.Printf("Successfully processed message: %s", msg.MessageID)
				}
			}),
		); err != nil && err != context.Canceled {
			log.Printf("Multi-consumer stopped with error: %v", err)
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down notification service...")
	cancel()
	time.Sleep(2 * time.Second) // Give consumers time to finish
	log.Println("Notification service stopped")
}

// User Service Notification Handlers

func handleUserRegistered(ctx context.Context, payload map[string]interface{}) error {
	userID, _ := payload["user_id"].(string)
	email, _ := payload["email"].(string)

	log.Printf("Sending welcome email to user %s at %s", userID, email)

	// TODO: Implement actual email sending logic
	// Example: sendEmail(email, "Welcome!", "Thank you for registering...")

	return nil
}

func handleUserPasswordReset(ctx context.Context, payload map[string]interface{}) error {
	userID, _ := payload["user_id"].(string)
	email, _ := payload["email"].(string)
	resetToken, _ := payload["reset_token"].(string)

	log.Printf("Sending password reset email to user %s at %s", userID, email)

	// TODO: Implement actual email sending logic
	fmt.Printf("Reset link: https://example.com/reset?token=%s\n", resetToken)

	return nil
}

func handleUserEmailVerified(ctx context.Context, payload map[string]interface{}) error {
	userID, _ := payload["user_id"].(string)
	email, _ := payload["email"].(string)

	log.Printf("Sending email verification confirmation to user %s at %s", userID, email)

	return nil
}

// Order Service Notification Handlers

func handleOrderCreated(ctx context.Context, payload map[string]interface{}) error {
	orderID, _ := payload["order_id"].(string)
	customerEmail, _ := payload["customer_email"].(string)
	total, _ := payload["total"].(float64)

	log.Printf("Sending order confirmation for order %s to %s (total: $%.2f)", orderID, customerEmail, total)

	// TODO: Implement actual notification logic
	// Could be email, SMS, push notification, etc.

	return nil
}

func handleOrderShipped(ctx context.Context, payload map[string]interface{}) error {
	orderID, _ := payload["order_id"].(string)
	customerEmail, _ := payload["customer_email"].(string)
	trackingNumber, _ := payload["tracking_number"].(string)

	log.Printf("Sending shipping notification for order %s to %s (tracking: %s)", orderID, customerEmail, trackingNumber)

	return nil
}

func handleOrderDelivered(ctx context.Context, payload map[string]interface{}) error {
	orderID, _ := payload["order_id"].(string)
	customerEmail, _ := payload["customer_email"].(string)

	log.Printf("Sending delivery confirmation for order %s to %s", orderID, customerEmail)

	return nil
}

// Payment Service Notification Handlers

func handlePaymentSuccessful(ctx context.Context, payload map[string]interface{}) error {
	paymentID, _ := payload["payment_id"].(string)
	customerEmail, _ := payload["customer_email"].(string)
	amount, _ := payload["amount"].(float64)

	log.Printf("Sending payment receipt for payment %s to %s (amount: $%.2f)", paymentID, customerEmail, amount)

	return nil
}

func handlePaymentFailed(ctx context.Context, payload map[string]interface{}) error {
	paymentID, _ := payload["payment_id"].(string)
	customerEmail, _ := payload["customer_email"].(string)
	reason, _ := payload["reason"].(string)

	log.Printf("Sending payment failure notification for payment %s to %s (reason: %s)", paymentID, customerEmail, reason)

	return nil
}

func handleRefundProcessed(ctx context.Context, payload map[string]interface{}) error {
	refundID, _ := payload["refund_id"].(string)
	customerEmail, _ := payload["customer_email"].(string)
	amount, _ := payload["amount"].(float64)

	log.Printf("Sending refund confirmation for refund %s to %s (amount: $%.2f)", refundID, customerEmail, amount)

	return nil
}
