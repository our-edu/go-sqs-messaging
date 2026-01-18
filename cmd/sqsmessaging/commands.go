package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/spf13/cobra"

	sqsdriver "github.com/our-edu/go-sqs-messaging/internal/drivers/sqs"
	"github.com/our-edu/go-sqs-messaging/internal/storage"
)

// createResolverWithCache creates a Resolver with Redis cache (used by CLI commands)
func createResolverWithCache(ctx context.Context, sqsClient *sqs.Client) (*sqsdriver.Resolver, error) {
	redisClient, err := createRedisClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis client: %w", err)
	}
	cache := storage.NewRedisCache(redisClient, "sqsmessaging")
	return sqsdriver.NewResolver(sqsClient, cfg, logger, cache), nil
}

// newEnsureCmd creates the ensure command (equivalent to sqs:ensure)
func newEnsureCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ensure",
		Short: "Ensure all configured SQS queues exist",
		Long: `Creates all queues defined in the configuration if they don't exist.
Each queue is created with an associated Dead Letter Queue (DLQ).

This command is useful for CI/CD pipelines to pre-create queues before deployment.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEnsure(cmd.Context())
		},
	}
}

func runEnsure(ctx context.Context) error {
	logger.Info().Msg("Ensuring SQS queues exist...")

	sqsClient, err := createSQSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	resolver, err := createResolverWithCache(ctx, sqsClient)
	if err != nil {
		return fmt.Errorf("failed to create resolver: %w", err)
	}

	// Get all queues from config
	queuesCreated := 0
	for serviceName, serviceQueues := range cfg.Queues {
		// Create default queue
		if serviceQueues.Default != "" {
			logger.Info().
				Str("service", serviceName).
				Str("queue", serviceQueues.Default).
				Msg("Ensuring queue")

			_, err := resolver.CreateQueueWithDLQ(ctx, serviceQueues.Default)
			if err != nil {
				logger.Error().
					Str("queue", serviceQueues.Default).
					Err(err).
					Msg("Failed to create queue")
			} else {
				queuesCreated++
			}
		}

		// Create specific queues
		for _, queueName := range serviceQueues.Specific {
			logger.Info().
				Str("service", serviceName).
				Str("queue", queueName).
				Msg("Ensuring queue")

			_, err := resolver.CreateQueueWithDLQ(ctx, queueName)
			if err != nil {
				logger.Error().
					Str("queue", queueName).
					Err(err).
					Msg("Failed to create queue")
			} else {
				queuesCreated++
			}
		}
	}

	logger.Info().Int("queues_created", queuesCreated).Msg("Queue creation complete")

	return nil
}

// newStatusCmd creates the status command (equivalent to sqs:status)
func newStatusCmd() *cobra.Command {
	var queueName string

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Display SQS queue status",
		Long:  `Shows the current status of SQS queues including message depth and DLQ depth.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStatus(cmd.Context(), queueName)
		},
	}

	cmd.Flags().StringVarP(&queueName, "queue", "q", "payment-service-queue", "Queue name to check")

	return cmd
}

func runStatus(ctx context.Context, queueName string) error {
	sqsClient, err := createSQSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	resolver, err := createResolverWithCache(ctx, sqsClient)
	if err != nil {
		return fmt.Errorf("failed to create resolver: %w", err)
	}
	consumer := sqsdriver.NewConsumer(sqsClient, resolver, cfg, logger)

	if err := consumer.SetQueue(ctx, queueName); err != nil {
		return fmt.Errorf("failed to set queue: %w", err)
	}

	// Get queue depth
	depth, err := consumer.GetQueueDepth(ctx)
	if err != nil {
		return fmt.Errorf("failed to get queue depth: %w", err)
	}

	// Get DLQ depth
	dlqDepth, err := consumer.GetDLQDepth(ctx, queueName)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to get DLQ depth")
		dlqDepth = -1
	}

	fmt.Printf("\n=== SQS Queue Status ===\n")
	fmt.Printf("Queue: %s-%s\n", cfg.SQS.Prefix, queueName)
	fmt.Printf("Messages in Queue: %d\n", depth)
	if dlqDepth >= 0 {
		fmt.Printf("Messages in DLQ: %d\n", dlqDepth)
		if dlqDepth > 0 {
			fmt.Printf("WARNING: DLQ has messages that need attention!\n")
		}
	}
	fmt.Printf("========================\n\n")

	return nil
}

// newCleanupCmd creates the cleanup command (equivalent to sqs:cleanup-processed-events)
func newCleanupCmd() *cobra.Command {
	var days int

	cmd := &cobra.Command{
		Use:   "cleanup",
		Short: "Clean up old processed event records",
		Long:  `Removes processed event records older than the specified number of days from the database.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCleanup(cmd.Context(), days)
		},
	}

	cmd.Flags().IntVarP(&days, "days", "d", 7, "Delete records older than this many days")

	return cmd
}

func runCleanup(ctx context.Context, days int) error {
	logger.Info().Int("older_than_days", days).Msg("Cleaning up processed events")

	store, err := createIdempotencyStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to create idempotency store: %w", err)
	}

	deleted, err := store.Cleanup(ctx, days)
	if err != nil {
		return fmt.Errorf("cleanup failed: %w", err)
	}

	fmt.Printf("Deleted %d processed event records older than %d days\n", deleted, days)
	return nil
}

// newInspectDlqCmd creates the inspect DLQ command (equivalent to sqs:inspect-dlq)
func newInspectDlqCmd() *cobra.Command {
	var limit int

	cmd := &cobra.Command{
		Use:   "inspect-dlq [queue]",
		Short: "Inspect messages in the Dead Letter Queue",
		Long:  `Views messages in the Dead Letter Queue for debugging purposes.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInspectDlq(cmd.Context(), args[0], limit)
		},
	}

	cmd.Flags().IntVarP(&limit, "limit", "l", 10, "Maximum messages to inspect")

	return cmd
}

func runInspectDlq(ctx context.Context, queueName string, limit int) error {
	sqsClient, err := createSQSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	resolver, err := createResolverWithCache(ctx, sqsClient)
	if err != nil {
		return fmt.Errorf("failed to create resolver: %w", err)
	}
	consumer := sqsdriver.NewConsumer(sqsClient, resolver, cfg, logger)

	messages, err := consumer.ReceiveFromDLQ(ctx, queueName, limit)
	if err != nil {
		return fmt.Errorf("failed to receive DLQ messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Println("No messages in DLQ")
		return nil
	}

	fmt.Printf("\n=== DLQ Messages for %s ===\n\n", queueName)
	for i, msg := range messages {
		fmt.Printf("--- Message %d ---\n", i+1)
		fmt.Printf("Message ID: %s\n", msg.MessageID)

		// Pretty print the body
		var prettyBody map[string]interface{}
		if err := json.Unmarshal([]byte(msg.Body), &prettyBody); err == nil {
			prettyJSON, _ := json.MarshalIndent(prettyBody, "", "  ")
			fmt.Printf("Body:\n%s\n", string(prettyJSON))
		} else {
			fmt.Printf("Body: %s\n", msg.Body)
		}
		fmt.Println()
	}

	return nil
}

// newMonitorDlqCmd creates the monitor DLQ command (equivalent to sqs:monitor-dlq)
func newMonitorDlqCmd() *cobra.Command {
	var alertThreshold int

	cmd := &cobra.Command{
		Use:   "monitor-dlq [queue]",
		Short: "Monitor Dead Letter Queue depth",
		Long:  `Checks DLQ depth and alerts if it exceeds the threshold.`,
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := "payment-service-queue"
			if len(args) > 0 {
				queueName = args[0]
			}
			return runMonitorDlq(cmd.Context(), queueName, alertThreshold)
		},
	}

	cmd.Flags().IntVarP(&alertThreshold, "threshold", "t", 10, "Alert threshold for DLQ depth")

	return cmd
}

func runMonitorDlq(ctx context.Context, queueName string, threshold int) error {
	sqsClient, err := createSQSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	resolver, err := createResolverWithCache(ctx, sqsClient)
	if err != nil {
		return fmt.Errorf("failed to create resolver: %w", err)
	}
	consumer := sqsdriver.NewConsumer(sqsClient, resolver, cfg, logger)

	depth, err := consumer.GetDLQDepth(ctx, queueName)
	if err != nil {
		return fmt.Errorf("failed to get DLQ depth: %w", err)
	}

	fmt.Printf("DLQ Depth for %s: %d\n", queueName, depth)

	if depth > int64(threshold) {
		fmt.Printf("ALERT: DLQ depth (%d) exceeds threshold (%d)!\n", depth, threshold)
		logger.Warn().
			Str("queue", queueName).
			Int64("depth", depth).
			Int("threshold", threshold).
			Msg("DLQ threshold exceeded")
	}

	return nil
}

// newReplayDlqCmd creates the replay DLQ command (equivalent to sqs:replay-dlq)
func newReplayDlqCmd() *cobra.Command {
	var limit int

	cmd := &cobra.Command{
		Use:   "replay-dlq [queue]",
		Short: "Replay messages from DLQ back to main queue",
		Long:  `Moves messages from the Dead Letter Queue back to the main queue for reprocessing.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReplayDlq(cmd.Context(), args[0], limit)
		},
	}

	cmd.Flags().IntVarP(&limit, "limit", "l", 10, "Maximum messages to replay")

	return cmd
}

func runReplayDlq(ctx context.Context, queueName string, limit int) error {
	sqsClient, err := createSQSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	resolver, err := createResolverWithCache(ctx, sqsClient)
	if err != nil {
		return fmt.Errorf("failed to create resolver: %w", err)
	}
	consumer := sqsdriver.NewConsumer(sqsClient, resolver, cfg, logger)

	// Receive messages from DLQ
	messages, err := consumer.ReceiveFromDLQ(ctx, queueName, limit)
	if err != nil {
		return fmt.Errorf("failed to receive DLQ messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Println("No messages in DLQ to replay")
		return nil
	}

	replayed := 0
	for _, msg := range messages {
		if err := consumer.ReplayFromDLQ(ctx, queueName, msg); err != nil {
			logger.Error().
				Str("message_id", msg.MessageID).
				Err(err).
				Msg("Failed to replay message")
		} else {
			replayed++
		}
	}

	fmt.Printf("Replayed %d messages from DLQ\n", replayed)
	return nil
}

// newTestConnectionCmd creates the test connection command (equivalent to sqs:test:connection)
func newTestConnectionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "test-connection",
		Short: "Test AWS SQS connection",
		Long:  `Validates AWS credentials and connectivity to SQS.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTestConnection(cmd.Context())
		},
	}
}

func runTestConnection(ctx context.Context) error {
	fmt.Println("Testing AWS SQS connection...")

	// Load AWS config
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.AWS.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AWS.AccessKeyID,
			cfg.AWS.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	sqsClient := sqs.NewFromConfig(awsCfg)

	// Try to list queues as a connection test
	_, err = sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{
		MaxResults: intPtr(1),
	})
	if err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}

	fmt.Println("Connection successful!")
	fmt.Printf("Region: %s\n", cfg.AWS.Region)
	fmt.Printf("Queue Prefix: %s\n", cfg.SQS.Prefix)

	return nil
}

// newTestReceiveCmd creates the test receive command (equivalent to sqs:test:receive)
func newTestReceiveCmd() *cobra.Command {
	var eventType string
	var send bool

	cmd := &cobra.Command{
		Use:   "test-receive [queue]",
		Short: "Test message sending and receiving",
		Long:  `Sends a test message and optionally receives it back.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runTestReceive(cmd.Context(), args[0], eventType, send)
		},
	}

	cmd.Flags().StringVar(&eventType, "event", "TestEvent", "Event type to use")
	cmd.Flags().BoolVar(&send, "send", false, "Also send a test message")

	return cmd
}

func runTestReceive(ctx context.Context, queueName, eventType string, send bool) error {
	sqsClient, err := createSQSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	resolver, err := createResolverWithCache(ctx, sqsClient)
	if err != nil {
		return fmt.Errorf("failed to create resolver: %w", err)
	}
	consumer := sqsdriver.NewConsumer(sqsClient, resolver, cfg, logger)

	if err := consumer.SetQueue(ctx, queueName); err != nil {
		return fmt.Errorf("failed to set queue: %w", err)
	}

	if send {
		// Send test message
		publisher := sqsdriver.NewPublisher(sqsClient, resolver, cfg, logger, "test-service")
		testPayload := map[string]interface{}{
			"test":      true,
			"timestamp": fmt.Sprintf("%d", time.Now().Unix()),
		}

		msgId, err := publisher.Publish(ctx, queueName, eventType, testPayload)
		if err != nil {
			return fmt.Errorf("failed to send test message: %w", err)
		}
		fmt.Printf("Sent test message: %s\n", msgId)
	}

	// Receive messages
	fmt.Println("Receiving messages...")
	messages, err := consumer.ReceiveMessages(ctx, 1, 5)
	if err != nil {
		return fmt.Errorf("failed to receive messages: %w", err)
	}

	if len(messages) == 0 {
		fmt.Println("No messages received")
		return nil
	}

	for _, msg := range messages {
		fmt.Printf("\nMessage ID: %s\n", msg.MessageID)
		fmt.Printf("Body: %s\n", msg.Body)
	}

	return nil
}

func intPtr(i int32) *int32 {
	return &i
}
