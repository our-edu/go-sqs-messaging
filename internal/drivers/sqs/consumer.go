package sqs

import (
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/our-edu/go-sqs-messaging/internal/config"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	"github.com/rs/zerolog"
)

// Consumer handles receiving messages from SQS queues
type Consumer struct {
	client   *sqs.Client
	resolver *Resolver
	config   *config.Config
	logger   zerolog.Logger
	queueURL string
}

// NewConsumer creates a new SQS consumer
func NewConsumer(client *sqs.Client, resolver *Resolver, cfg *config.Config, logger zerolog.Logger) *Consumer {
	return &Consumer{
		client:   client,
		resolver: resolver,
		config:   cfg,
		logger:   logger,
	}
}

// SetQueue sets the queue to consume from
func (c *Consumer) SetQueue(ctx context.Context, queueName string) error {
	url, err := c.resolver.Resolve(ctx, queueName)
	if err != nil {
		return fmt.Errorf("failed to resolve queue: %w", err)
	}
	c.queueURL = url
	return nil
}

// ReceiveMessages polls for messages from the queue
func (c *Consumer) ReceiveMessages(ctx context.Context, maxMessages int, waitTime int) ([]contracts.Message, error) {
	if c.queueURL == "" {
		return nil, fmt.Errorf("queue not set, call SetQueue first")
	}

	if maxMessages > 10 {
		maxMessages = 10
	}

	result, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(c.queueURL),
		MaxNumberOfMessages:   int32(maxMessages),
		WaitTimeSeconds:       int32(waitTime),
		VisibilityTimeout:     int32(c.config.SQS.VisibilityTimeout),
		MessageAttributeNames: []string{"All"},
		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to receive messages: %w", err)
	}

	messages := make([]contracts.Message, len(result.Messages))
	for i, msg := range result.Messages {
		messages[i] = contracts.Message{
			MessageID:         aws.ToString(msg.MessageId),
			ReceiptHandle:     aws.ToString(msg.ReceiptHandle),
			Body:              aws.ToString(msg.Body),
			Attributes:        msg.Attributes,
			MessageAttributes: convertMessageAttributes(msg.MessageAttributes),
		}
	}

	if len(messages) > 0 {
		c.logger.Debug().
			Int("count", len(messages)).
			Str("queue", c.queueURL).
			Msg("Received messages")
	}

	return messages, nil
}

// DeleteMessage acknowledges and removes a message from the queue
func (c *Consumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	if c.queueURL == "" {
		return fmt.Errorf("queue not set, call SetQueue first")
	}

	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}
	return nil
}

// ChangeVisibilityTimeout extends the visibility timeout for a message
func (c *Consumer) ChangeVisibilityTimeout(ctx context.Context, receiptHandle string, timeout int) error {
	if c.queueURL == "" {
		return fmt.Errorf("queue not set, call SetQueue first")
	}

	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: int32(timeout),
	})
	if err != nil {
		return fmt.Errorf("failed to change visibility timeout: %w", err)
	}
	return nil
}

// GetQueueDepth returns the approximate number of messages in the queue
func (c *Consumer) GetQueueDepth(ctx context.Context) (int64, error) {
	if c.queueURL == "" {
		return 0, fmt.Errorf("queue not set, call SetQueue first")
	}

	result, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(c.queueURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get queue attributes: %w", err)
	}

	countStr := result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse message count: %w", err)
	}

	return count, nil
}

// GetDLQDepth returns the approximate number of messages in the DLQ
func (c *Consumer) GetDLQDepth(ctx context.Context, queueName string) (int64, error) {
	dlqURL, err := c.resolver.GetDLQUrl(ctx, queueName)
	if err != nil {
		return 0, err
	}

	result, err := c.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get DLQ attributes: %w", err)
	}

	countStr := result.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)]
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse DLQ message count: %w", err)
	}

	return count, nil
}

// ReceiveFromDLQ receives messages from the Dead Letter Queue
func (c *Consumer) ReceiveFromDLQ(ctx context.Context, queueName string, maxMessages int) ([]contracts.Message, error) {
	dlqURL, err := c.resolver.GetDLQUrl(ctx, queueName)
	if err != nil {
		return nil, err
	}

	if maxMessages > 10 {
		maxMessages = 10
	}

	result, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		MaxNumberOfMessages:   int32(maxMessages),
		WaitTimeSeconds:       0, // No long polling for DLQ inspection
		MessageAttributeNames: []string{"All"},
		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to receive DLQ messages: %w", err)
	}

	messages := make([]contracts.Message, len(result.Messages))
	for i, msg := range result.Messages {
		messages[i] = contracts.Message{
			MessageID:         aws.ToString(msg.MessageId),
			ReceiptHandle:     aws.ToString(msg.ReceiptHandle),
			Body:              aws.ToString(msg.Body),
			Attributes:        msg.Attributes,
			MessageAttributes: convertMessageAttributes(msg.MessageAttributes),
		}
	}

	return messages, nil
}

// ReplayFromDLQ moves a message from DLQ back to the main queue
func (c *Consumer) ReplayFromDLQ(ctx context.Context, queueName string, message contracts.Message) error {
	// Get main queue URL
	mainQueueURL, err := c.resolver.Resolve(ctx, queueName)
	if err != nil {
		return fmt.Errorf("failed to resolve main queue: %w", err)
	}

	// Get DLQ URL
	dlqURL, err := c.resolver.GetDLQUrl(ctx, queueName)
	if err != nil {
		return err
	}

	// Send to main queue
	_, err = c.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(mainQueueURL),
		MessageBody: aws.String(message.Body),
	})
	if err != nil {
		return fmt.Errorf("failed to send message to main queue: %w", err)
	}

	// Delete from DLQ
	_, err = c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(dlqURL),
		ReceiptHandle: aws.String(message.ReceiptHandle),
	})
	if err != nil {
		return fmt.Errorf("failed to delete message from DLQ: %w", err)
	}

	c.logger.Info().
		Str("message_id", message.MessageID).
		Str("queue", queueName).
		Msg("Replayed message from DLQ")

	return nil
}

func convertMessageAttributes(attrs map[string]types.MessageAttributeValue) map[string]contracts.MessageAttribute {
	result := make(map[string]contracts.MessageAttribute)
	for k, v := range attrs {
		result[k] = contracts.MessageAttribute{
			DataType: aws.ToString(v.DataType),
			Value:    aws.ToString(v.StringValue),
		}
	}
	return result
}
