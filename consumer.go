package sqsmessaging

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/our-edu/go-sqs-messaging/internal/contracts"
	"github.com/our-edu/go-sqs-messaging/internal/metrics"
	"github.com/our-edu/go-sqs-messaging/pkg/envelope"
)

// runConsumerLoop is the main consumer loop that processes messages from a single queue.
func (c *Client) runConsumerLoop(ctx context.Context, queueName string, opts *consumerOptions) error {
	var stats consumerStats
	stats.queueName = queueName

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().
				Int("total_processed", stats.totalProcessed).
				Int("success", stats.success).
				Int("validation_errors", stats.validationErrors).
				Int("transient_errors", stats.transientErrors).
				Int("permanent_errors", stats.permanentErrors).
				Msg("Consumer shutting down")
			return ctx.Err()
		default:
		}

		// Receive messages
		messages, err := c.consumer.ReceiveMessages(ctx, opts.maxMessages, opts.waitTime)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Error().Err(err).Msg("Failed to receive messages")
			if opts.onError != nil {
				opts.onError(err)
			}
			continue
		}

		if len(messages) == 0 {
			continue
		}

		c.logger.Debug().Int("count", len(messages)).Msg("Received messages")

		// Process each message
		for _, msg := range messages {
			stats.totalProcessed++

			// Convert to public Message type
			pubMsg := Message{
				MessageID:     msg.MessageID,
				ReceiptHandle: msg.ReceiptHandle,
				Body:          msg.Body,
				Attributes:    msg.Attributes,
			}

			if opts.onMessageStart != nil {
				opts.onMessageStart(pubMsg)
			}

			processErr := c.processMessage(ctx, msg, opts, &stats)

			if opts.onMessageEnd != nil {
				opts.onMessageEnd(pubMsg, processErr)
			}

			c.handleProcessResult(ctx, msg, processErr, &stats)
		}

		// Check error rates periodically
		c.checkErrorRates(&stats)
	}
}

type consumerStats struct {
	queueName        string
	totalProcessed   int
	success          int
	validationErrors int
	transientErrors  int
	permanentErrors  int
}

func (c *Client) processMessage(ctx context.Context, msg contracts.Message, opts *consumerOptions, stats *consumerStats) error {
	return c.processInternalMessage(ctx, msg, opts)
}

func (c *Client) processInternalMessage(ctx context.Context, msg contracts.Message, opts *consumerOptions) error {
	body := msg.Body
	receiptHandle := msg.ReceiptHandle

	// Parse envelope
	env, err := envelope.ParseEnvelopeFromMessage(body)
	if err != nil {
		return NewValidationError("invalid envelope", err)
	}

	// Validate envelope
	if err := env.Validate(); err != nil {
		return NewValidationError("envelope validation failed", err)
	}

	eventType := env.GetEventType()
	idempotencyKey := env.GetIdempotencyKey()

	c.logger.Debug().
		Str("event_type", eventType).
		Str("idempotency_key", idempotencyKey).
		Str("trace_id", env.GetTraceID()).
		Msg("Processing message")

	// Check idempotency if store is configured
	if c.idempotencyStore != nil {
		processed, err := c.idempotencyStore.IsProcessed(ctx, idempotencyKey)
		if err != nil {
			return NewTransientError("idempotency check failed", err)
		}
		if processed {
			c.logger.Info().
				Str("idempotency_key", idempotencyKey).
				Msg("Message already processed, skipping")
			return nil
		}

		// Mark as processing
		if err := c.idempotencyStore.MarkProcessing(ctx, idempotencyKey); err != nil {
			c.logger.Info().
				Str("idempotency_key", idempotencyKey).
				Msg("Message is being processed by another consumer")
			return nil // Not an error, just skip
		}
		defer c.idempotencyStore.ClearProcessing(ctx, idempotencyKey)
	}

	// Extend visibility for long-running events
	if c.config.IsLongRunningEvent(eventType) {
		c.consumer.ChangeVisibilityTimeout(ctx, receiptHandle, 300) // 5 minutes
	}

	// Get handler
	handler, ok := c.eventRegistry.GetHandler(eventType)
	if !ok {
		return NewPermanentError("no handler registered for event type: "+eventType, nil)
	}

	// Execute handler
	startTime := time.Now()
	if err := handler(ctx, env.Payload); err != nil {
		// Classify the error
		if isTransientErr(err) {
			return NewTransientError("handler failed", err)
		}
		return NewPermanentError("handler failed", err)
	}
	duration := time.Since(startTime)

	// Record metrics
	if c.metricsService != nil {
		c.metricsService.RecordDuration(ctx, metrics.MetricProcessingTime, float64(duration.Milliseconds()), map[string]string{
			"event_type": eventType,
		})
		c.metricsService.Increment(ctx, metrics.MetricMessagesSuccess, map[string]string{
			"event_type": eventType,
		})
	}

	// Mark as processed
	if c.idempotencyStore != nil {
		if err := c.idempotencyStore.MarkProcessed(ctx, idempotencyKey, eventType, env.Service); err != nil {
			c.logger.Warn().
				Str("idempotency_key", idempotencyKey).
				Err(err).
				Msg("Failed to mark as processed")
		}
	}

	c.logger.Info().
		Str("event_type", eventType).
		Dur("duration", duration).
		Msg("Message processed successfully")

	return nil
}

func (c *Client) handleProcessResult(ctx context.Context, msg contracts.Message, err error, stats *consumerStats) {
	receiptHandle := msg.ReceiptHandle
	messageID := msg.MessageID

	if err == nil {
		stats.success++
		// Delete successful message
		if deleteErr := c.consumer.DeleteMessage(ctx, receiptHandle); deleteErr != nil {
			c.logger.Error().
				Str("message_id", messageID).
				Err(deleteErr).
				Msg("Failed to delete message")
		}
		return
	}

	switch ClassifyError(err) {
	case ErrorTypeValidation:
		stats.validationErrors++
		c.logger.Warn().
			Str("message_id", messageID).
			Err(err).
			Msg("Validation error, deleting message")
		c.consumer.DeleteMessage(ctx, receiptHandle)
		if c.metricsService != nil {
			c.metricsService.Increment(ctx, metrics.MetricValidationErrors, nil)
		}

	case ErrorTypeTransient:
		stats.transientErrors++
		c.logger.Warn().
			Str("message_id", messageID).
			Err(err).
			Msg("Transient error, leaving for retry")
		// Don't delete - will be retried or sent to DLQ
		if c.metricsService != nil {
			c.metricsService.Increment(ctx, metrics.MetricTransientErrors, nil)
		}

	case ErrorTypePermanent:
		stats.permanentErrors++
		c.logger.Error().
			Str("message_id", messageID).
			Err(err).
			Msg("Permanent error, deleting message")
		c.consumer.DeleteMessage(ctx, receiptHandle)
		if c.metricsService != nil {
			c.metricsService.Increment(ctx, metrics.MetricPermanentErrors, nil)
		}

	default:
		c.logger.Error().
			Str("message_id", messageID).
			Err(err).
			Msg("Unknown error")
	}
}

func (c *Client) checkErrorRates(stats *consumerStats) {
	if stats.totalProcessed < 100 {
		return // Need enough samples
	}

	validationRate := float64(stats.validationErrors) / float64(stats.totalProcessed) * 100
	transientRate := float64(stats.transientErrors) / float64(stats.totalProcessed) * 100

	if validationRate > 1.0 {
		c.logger.Warn().
			Float64("rate_percent", validationRate).
			Int("validation_errors", stats.validationErrors).
			Int("total", stats.totalProcessed).
			Msg("High validation error rate")
	}

	if transientRate > 10.0 {
		c.logger.Warn().
			Float64("rate_percent", transientRate).
			Int("transient_errors", stats.transientErrors).
			Int("total", stats.totalProcessed).
			Msg("High transient error rate")
	}
}

func isTransientErr(err error) bool {
	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	// Check for common transient error messages
	errMsg := strings.ToLower(err.Error())
	transientPatterns := []string{
		"connection",
		"timeout",
		"temporary",
		"unavailable",
		"retry",
		"throttl",
	}
	for _, pattern := range transientPatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}
	return false
}

// queueConsumer represents a consumer for a single queue in the multi-consumer setup
type queueConsumer struct {
	queueName string
	queueURL  string
}

// runMultiConsumerLoop starts multiple goroutines to consume from multiple queues.
// It uses a worker pool pattern where each worker polls one queue.
func (c *Client) runMultiConsumerLoop(ctx context.Context, queueNames []string, opts *consumerOptions) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(queueNames))

	// Create a context that we can cancel if any consumer fails fatally
	consumerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Resolve all queue URLs upfront
	queueConsumers := make([]queueConsumer, 0, len(queueNames))
	for _, queueName := range queueNames {
		url, err := c.resolver.Resolve(consumerCtx, queueName)
		if err != nil {
			return fmt.Errorf("failed to resolve queue %s: %w", queueName, err)
		}
		queueConsumers = append(queueConsumers, queueConsumer{
			queueName: queueName,
			queueURL:  url,
		})
	}

	// Start a worker for each queue
	for _, qc := range queueConsumers {
		wg.Add(1)
		go func(qc queueConsumer) {
			defer wg.Done()

			err := c.runSingleQueueConsumer(consumerCtx, qc, opts)
			if err != nil && err != context.Canceled {
				errChan <- fmt.Errorf("queue %s consumer error: %w", qc.queueName, err)
			}
		}(qc)

		c.logger.Info().
			Str("queue", qc.queueName).
			Str("url", qc.queueURL).
			Msg("Started queue consumer worker")
	}

	// Wait for all workers to finish or context cancellation
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All workers finished
		close(errChan)
		// Collect any errors
		var errs []error
		for err := range errChan {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			return fmt.Errorf("multi-consumer errors: %v", errs)
		}
		return nil
	case <-ctx.Done():
		c.logger.Info().Msg("Multi-consumer shutting down")
		cancel() // Signal all workers to stop
		<-done   // Wait for cleanup
		return ctx.Err()
	}
}

// runSingleQueueConsumer handles consuming from a single queue in the multi-consumer setup
func (c *Client) runSingleQueueConsumer(ctx context.Context, qc queueConsumer, opts *consumerOptions) error {
	var stats consumerStats
	stats.queueName = qc.queueName

	for {
		select {
		case <-ctx.Done():
			c.logger.Info().
				Str("queue", qc.queueName).
				Int("total_processed", stats.totalProcessed).
				Int("success", stats.success).
				Int("validation_errors", stats.validationErrors).
				Int("transient_errors", stats.transientErrors).
				Int("permanent_errors", stats.permanentErrors).
				Msg("Queue consumer shutting down")
			return ctx.Err()
		default:
		}

		// Receive messages from this queue
		messages, err := c.receiveMessagesFromURL(ctx, qc.queueURL, opts.maxMessages, opts.waitTime)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Error().
				Str("queue", qc.queueName).
				Err(err).
				Msg("Failed to receive messages")
			if opts.onError != nil {
				opts.onError(err)
			}
			continue
		}

		if len(messages) == 0 {
			continue
		}

		c.logger.Debug().
			Str("queue", qc.queueName).
			Int("count", len(messages)).
			Msg("Received messages")

		// Process each message
		for _, msg := range messages {
			stats.totalProcessed++

			// Convert to public Message type
			pubMsg := Message{
				MessageID:     msg.MessageID,
				ReceiptHandle: msg.ReceiptHandle,
				Body:          msg.Body,
				Attributes:    msg.Attributes,
			}

			if opts.onMessageStart != nil {
				opts.onMessageStart(pubMsg)
			}

			processErr := c.processMessageWithURL(ctx, qc.queueURL, msg, opts)

			if opts.onMessageEnd != nil {
				opts.onMessageEnd(pubMsg, processErr)
			}

			c.handleProcessResultWithURL(ctx, qc.queueURL, msg, processErr, &stats)
		}

		// Check error rates periodically
		c.checkErrorRates(&stats)
	}
}

// receiveMessagesFromURL receives messages from a specific queue URL
func (c *Client) receiveMessagesFromURL(ctx context.Context, queueURL string, maxMessages, waitTime int) ([]contracts.Message, error) {
	if maxMessages > 10 {
		maxMessages = 10
	}

	result, err := c.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   int32(maxMessages),
		WaitTimeSeconds:       int32(waitTime),
		VisibilityTimeout:     int32(c.config.SQS.VisibilityTimeout),
		MessageAttributeNames: []string{"All"},
		AttributeNames:        []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
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
			MessageAttributes: c.convertMessageAttributes(msg.MessageAttributes),
		}
	}

	return messages, nil
}

// processMessageWithURL processes a message and uses a specific queue URL for operations
func (c *Client) processMessageWithURL(ctx context.Context, queueURL string, msg contracts.Message, opts *consumerOptions) error {
	body := msg.Body
	receiptHandle := msg.ReceiptHandle

	// Parse envelope
	env, err := envelope.ParseEnvelopeFromMessage(body)
	if err != nil {
		return NewValidationError("invalid envelope", err)
	}

	// Validate envelope
	if err := env.Validate(); err != nil {
		return NewValidationError("envelope validation failed", err)
	}

	eventType := env.GetEventType()
	idempotencyKey := env.GetIdempotencyKey()

	c.logger.Debug().
		Str("event_type", eventType).
		Str("idempotency_key", idempotencyKey).
		Str("trace_id", env.GetTraceID()).
		Msg("Processing message")

	// Check idempotency if store is configured
	if c.idempotencyStore != nil {
		processed, err := c.idempotencyStore.IsProcessed(ctx, idempotencyKey)
		if err != nil {
			return NewTransientError("idempotency check failed", err)
		}
		if processed {
			c.logger.Info().
				Str("idempotency_key", idempotencyKey).
				Msg("Message already processed, skipping")
			return nil
		}

		// Mark as processing
		if err := c.idempotencyStore.MarkProcessing(ctx, idempotencyKey); err != nil {
			c.logger.Info().
				Str("idempotency_key", idempotencyKey).
				Msg("Message is being processed by another consumer")
			return nil // Not an error, just skip
		}
		defer c.idempotencyStore.ClearProcessing(ctx, idempotencyKey)
	}

	// Extend visibility for long-running events
	if c.config.IsLongRunningEvent(eventType) {
		c.changeVisibilityTimeoutWithURL(ctx, queueURL, receiptHandle, 300) // 5 minutes
	}

	// Get handler
	handler, ok := c.eventRegistry.GetHandler(eventType)
	if !ok {
		return NewPermanentError("no handler registered for event type: "+eventType, nil)
	}

	// Execute handler
	startTime := time.Now()
	if err := handler(ctx, env.Payload); err != nil {
		// Classify the error
		if isTransientErr(err) {
			return NewTransientError("handler failed", err)
		}
		return NewPermanentError("handler failed", err)
	}
	duration := time.Since(startTime)

	// Record metrics
	if c.metricsService != nil {
		c.metricsService.RecordDuration(ctx, metrics.MetricProcessingTime, float64(duration.Milliseconds()), map[string]string{
			"event_type": eventType,
		})
		c.metricsService.Increment(ctx, metrics.MetricMessagesSuccess, map[string]string{
			"event_type": eventType,
		})
	}

	// Mark as processed
	if c.idempotencyStore != nil {
		if err := c.idempotencyStore.MarkProcessed(ctx, idempotencyKey, eventType, env.Service); err != nil {
			c.logger.Warn().
				Str("idempotency_key", idempotencyKey).
				Err(err).
				Msg("Failed to mark as processed")
		}
	}

	c.logger.Info().
		Str("event_type", eventType).
		Dur("duration", duration).
		Msg("Message processed successfully")

	return nil
}

// handleProcessResultWithURL handles the result of processing a message using a specific queue URL
func (c *Client) handleProcessResultWithURL(ctx context.Context, queueURL string, msg contracts.Message, err error, stats *consumerStats) {
	receiptHandle := msg.ReceiptHandle
	messageID := msg.MessageID

	if err == nil {
		stats.success++
		// Delete successful message
		if deleteErr := c.deleteMessageWithURL(ctx, queueURL, receiptHandle); deleteErr != nil {
			c.logger.Error().
				Str("message_id", messageID).
				Err(deleteErr).
				Msg("Failed to delete message")
		}
		return
	}

	switch ClassifyError(err) {
	case ErrorTypeValidation:
		stats.validationErrors++
		c.logger.Warn().
			Str("message_id", messageID).
			Err(err).
			Msg("Validation error, deleting message")
		c.deleteMessageWithURL(ctx, queueURL, receiptHandle)
		if c.metricsService != nil {
			c.metricsService.Increment(ctx, metrics.MetricValidationErrors, nil)
		}

	case ErrorTypeTransient:
		stats.transientErrors++
		c.logger.Warn().
			Str("message_id", messageID).
			Err(err).
			Msg("Transient error, leaving for retry")
		// Don't delete - will be retried or sent to DLQ
		if c.metricsService != nil {
			c.metricsService.Increment(ctx, metrics.MetricTransientErrors, nil)
		}

	case ErrorTypePermanent:
		stats.permanentErrors++
		c.logger.Error().
			Str("message_id", messageID).
			Err(err).
			Msg("Permanent error, deleting message")
		c.deleteMessageWithURL(ctx, queueURL, receiptHandle)
		if c.metricsService != nil {
			c.metricsService.Increment(ctx, metrics.MetricPermanentErrors, nil)
		}

	default:
		c.logger.Error().
			Str("message_id", messageID).
			Err(err).
			Msg("Unknown error")
	}
}

// deleteMessageWithURL deletes a message from a specific queue URL
func (c *Client) deleteMessageWithURL(ctx context.Context, queueURL, receiptHandle string) error {
	_, err := c.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}
	return nil
}

// changeVisibilityTimeoutWithURL changes the visibility timeout for a message
func (c *Client) changeVisibilityTimeoutWithURL(ctx context.Context, queueURL, receiptHandle string, timeout int) error {
	_, err := c.sqsClient.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: int32(timeout),
	})
	if err != nil {
		return fmt.Errorf("failed to change visibility timeout: %w", err)
	}
	return nil
}

// convertMessageAttributes converts SQS message attributes to contracts format
func (c *Client) convertMessageAttributes(attrs map[string]sqstypes.MessageAttributeValue) map[string]contracts.MessageAttribute {
	result := make(map[string]contracts.MessageAttribute)
	for k, v := range attrs {
		result[k] = contracts.MessageAttribute{
			DataType: aws.ToString(v.DataType),
			Value:    aws.ToString(v.StringValue),
		}
	}
	return result
}
