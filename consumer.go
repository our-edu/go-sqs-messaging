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
	"github.com/our-edu/go-sqs-messaging/pkg/envelope"
)

// runConsumerLoop is the main consumer loop that processes messages from a single queue.
func (c *Client) runConsumerLoop(ctx context.Context, queueName string, opts *consumerOptions) error {
	var stats consumerStats
	stats.queueName = queueName
	consecutiveErrors := 0

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
			consecutiveErrors++
			c.logger.Error().Err(err).Msg("Failed to receive messages")
			if opts.onError != nil {
				opts.onError(err)
			}

			// Check if queue doesn't exist (e.g., LocalStack restarted)
			if isNonExistentQueueErr(err) && opts.createIfNotExists {
				c.logger.Warn().
					Str("queue", queueName).
					Msg("Queue does not exist, attempting to recreate")

				// Clear cache and recreate queue
				if clearErr := c.resolver.ClearCache(ctx); clearErr != nil {
					c.logger.Warn().Err(clearErr).Msg("Failed to clear queue cache")
				}

				if _, createErr := c.resolver.CreateQueueWithDLQ(ctx, queueName); createErr != nil {
					c.logger.Error().Err(createErr).Msg("Failed to recreate queue")
				} else {
					// Re-set the queue URL in consumer
					if setErr := c.consumer.SetQueue(ctx, queueName); setErr != nil {
						c.logger.Error().Err(setErr).Msg("Failed to set queue after recreation")
					} else {
						c.logger.Info().Str("queue", queueName).Msg("Queue recreated successfully")
					}
				}
			}

			// Apply backoff delay before retrying
			delay := calculateBackoffDelay(
				consecutiveErrors,
				opts.errorBackoff.initialDelay,
				opts.errorBackoff.maxDelay,
				opts.errorBackoff.multiplier,
			)
			if delay > 0 {
				c.logger.Debug().
					Dur("backoff_delay", delay).
					Int("consecutive_errors", consecutiveErrors).
					Msg("Waiting before retry")
				if err := waitWithBackoff(ctx, delay); err != nil {
					return err
				}
			}
			continue
		}

		// Reset consecutive errors on successful receive
		consecutiveErrors = 0

		if len(messages) == 0 {
			continue
		}

		c.logger.Debug().Int("count", len(messages)).Msg("Received messages")

		maxConc := opts.maxConcurrency
		if maxConc < 1 {
			maxConc = 1
		}

		sem := make(chan struct{}, maxConc)
		var wg sync.WaitGroup

		// Process each message concurrently
		for _, msg := range messages {
			wg.Add(1)
			sem <- struct{}{}

			go func(m contracts.Message) {
				defer wg.Done()
				defer func() { <-sem }()

				stats.mu.Lock()
				stats.totalProcessed++
				stats.mu.Unlock()

				pubMsg := Message{
					MessageID:     m.MessageID,
					ReceiptHandle: m.ReceiptHandle,
					Body:          m.Body,
					Attributes:    m.Attributes,
				}

				if opts.onMessageStart != nil {
					opts.onMessageStart(pubMsg)
				}

				processErr := c.processMessage(ctx, m, opts, &stats, queueName)

				if opts.onMessageEnd != nil {
					opts.onMessageEnd(pubMsg, processErr)
				}

				c.handleProcessResult(ctx, m, processErr, &stats)
			}(msg)
		}

		wg.Wait()

		// Check error rates periodically
		c.checkErrorRates(&stats)
	}
}

type consumerStats struct {
	mu               sync.Mutex
	queueName        string
	totalProcessed   int
	success          int
	validationErrors int
	transientErrors  int
	permanentErrors  int
}

func (c *Client) processMessage(ctx context.Context, msg contracts.Message, opts *consumerOptions, stats *consumerStats, queueName string) error {
	return c.processInternalMessage(ctx, msg, opts, queueName)
}

func (c *Client) processInternalMessage(ctx context.Context, msg contracts.Message, opts *consumerOptions, queueName string) error {
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

	// Change visibility timeout based on event type configuration (before processing starts)
	if timeout, exists := c.config.GetEventTimeout(eventType); exists {
		if err := c.consumer.ChangeVisibilityTimeout(ctx, receiptHandle, timeout); err != nil {
			c.logger.Warn().
				Str("event_type", eventType).
				Int("timeout", timeout).
				Err(err).
				Msg("Failed to change visibility timeout for event")
		} else {
			c.logger.Debug().
				Str("event_type", eventType).
				Int("timeout", timeout).
				Msg("Changed visibility timeout for event")
		}
	} else if c.config.IsLongRunningEvent(eventType) {
		// Fallback to legacy long-running events (5 minutes)
		c.consumer.ChangeVisibilityTimeout(ctx, receiptHandle, 300)
	}

	// Get handler
	handler, ok := c.eventRegistry.GetHandler(eventType)
	if !ok {
		return NewPermanentError("no handler registered for event type: "+eventType, nil)
	}

	// Create context with message metadata
	handlerCtx := context.WithValue(ctx, ContextKeySourceService, env.Service)
	handlerCtx = context.WithValue(handlerCtx, ContextKeyEventType, eventType)
	handlerCtx = context.WithValue(handlerCtx, ContextKeyTraceID, env.GetTraceID())
	handlerCtx = context.WithValue(handlerCtx, ContextKeyMessageID, msg.MessageID)
	handlerCtx = context.WithValue(handlerCtx, ContextKeyQueueName, queueName)

	// Execute handler
	startTime := time.Now()
	if err := handler(handlerCtx, env.Payload); err != nil {
		// Classify the error
		if isTransientErr(err) {
			return NewTransientError("handler failed", err)
		}
		return NewPermanentError("handler failed", err)
	}
	duration := time.Since(startTime)

	// Record metrics using unified provider
	c.metricsProvider.ObserveProcessingDuration(ctx, queueName, eventType, float64(duration.Milliseconds()))
	c.metricsProvider.IncMessagesSuccess(ctx, queueName, eventType)

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
		stats.mu.Lock()
		stats.success++
		stats.mu.Unlock()
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
		stats.mu.Lock()
		stats.validationErrors++
		stats.mu.Unlock()
		c.logger.Warn().
			Str("message_id", messageID).
			Err(err).
			Msg("Validation error, deleting message")
		c.consumer.DeleteMessage(ctx, receiptHandle)
		c.metricsProvider.IncValidationErrors(ctx, stats.queueName, "")

	case ErrorTypeTransient:
		stats.mu.Lock()
		stats.transientErrors++
		stats.mu.Unlock()
		c.logger.Warn().
			Str("message_id", messageID).
			Err(err).
			Msg("Transient error, leaving for retry")
		// Don't delete - will be retried or sent to DLQ
		c.metricsProvider.IncTransientErrors(ctx, stats.queueName, "")

	case ErrorTypePermanent:
		stats.mu.Lock()
		stats.permanentErrors++
		stats.mu.Unlock()
		c.logger.Error().
			Str("message_id", messageID).
			Err(err).
			Msg("Permanent error, deleting message")
		c.consumer.DeleteMessage(ctx, receiptHandle)
		c.metricsProvider.IncPermanentErrors(ctx, stats.queueName, "")

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

// calculateBackoffDelay calculates the delay for exponential backoff
func calculateBackoffDelay(consecutiveErrors int, initialDelay, maxDelay time.Duration, multiplier float64) time.Duration {
	if consecutiveErrors <= 0 {
		return 0
	}

	delay := initialDelay
	for i := 1; i < consecutiveErrors; i++ {
		delay = time.Duration(float64(delay) * multiplier)
		if delay > maxDelay {
			return maxDelay
		}
	}
	return delay
}

// waitWithBackoff waits for the calculated backoff delay, respecting context cancellation
func waitWithBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
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

// isNonExistentQueueErr checks if the error is due to a non-existent queue
func isNonExistentQueueErr(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "NonExistentQueue") ||
		strings.Contains(errMsg, "queue does not exist")
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
	for i := range queueConsumers {
		wg.Add(1)
		go func(qc *queueConsumer) {
			defer wg.Done()

			err := c.runSingleQueueConsumer(consumerCtx, qc, opts)
			if err != nil && err != context.Canceled {
				errChan <- fmt.Errorf("queue %s consumer error: %w", qc.queueName, err)
			}
		}(&queueConsumers[i])

		c.logger.Info().
			Str("queue", queueConsumers[i].queueName).
			Str("url", queueConsumers[i].queueURL).
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
func (c *Client) runSingleQueueConsumer(ctx context.Context, qc *queueConsumer, opts *consumerOptions) error {
	var stats consumerStats
	stats.queueName = qc.queueName
	consecutiveErrors := 0

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
			consecutiveErrors++
			c.logger.Error().
				Str("queue", qc.queueName).
				Err(err).
				Msg("Failed to receive messages")
			if opts.onError != nil {
				opts.onError(err)
			}

			// Check if queue doesn't exist (e.g., LocalStack restarted)
			if isNonExistentQueueErr(err) && opts.createIfNotExists {
				c.logger.Warn().
					Str("queue", qc.queueName).
					Msg("Queue does not exist, attempting to recreate")

				// Clear cache and recreate queue
				if clearErr := c.resolver.ClearCache(ctx); clearErr != nil {
					c.logger.Warn().Err(clearErr).Msg("Failed to clear queue cache")
				}

				if newURL, createErr := c.resolver.CreateQueueWithDLQ(ctx, qc.queueName); createErr != nil {
					c.logger.Error().Err(createErr).Msg("Failed to recreate queue")
				} else {
					// Update the queue URL
					qc.queueURL = newURL
					c.logger.Info().
						Str("queue", qc.queueName).
						Str("url", newURL).
						Msg("Queue recreated successfully")
				}
			}

			// Apply backoff delay before retrying
			delay := calculateBackoffDelay(
				consecutiveErrors,
				opts.errorBackoff.initialDelay,
				opts.errorBackoff.maxDelay,
				opts.errorBackoff.multiplier,
			)
			if delay > 0 {
				c.logger.Debug().
					Str("queue", qc.queueName).
					Dur("backoff_delay", delay).
					Int("consecutive_errors", consecutiveErrors).
					Msg("Waiting before retry")
				if err := waitWithBackoff(ctx, delay); err != nil {
					return err
				}
			}
			continue
		}

		// Reset consecutive errors on successful receive
		consecutiveErrors = 0

		if len(messages) == 0 {
			continue
		}

		c.logger.Debug().
			Str("queue", qc.queueName).
			Int("count", len(messages)).
			Msg("Received messages")

		maxConc := opts.maxConcurrency
		if maxConc < 1 {
			maxConc = 1
		}

		sem := make(chan struct{}, maxConc)
		var wg sync.WaitGroup

		// Process each message concurrently
		for _, msg := range messages {
			wg.Add(1)
			sem <- struct{}{}

			go func(m contracts.Message) {
				defer wg.Done()
				defer func() { <-sem }()

				stats.mu.Lock()
				stats.totalProcessed++
				stats.mu.Unlock()

				pubMsg := Message{
					MessageID:     m.MessageID,
					ReceiptHandle: m.ReceiptHandle,
					Body:          m.Body,
					Attributes:    m.Attributes,
				}

				if opts.onMessageStart != nil {
					opts.onMessageStart(pubMsg)
				}

				processErr := c.processMessageWithURL(ctx, qc.queueURL, m, opts)

				if opts.onMessageEnd != nil {
					opts.onMessageEnd(pubMsg, processErr)
				}

				c.handleProcessResultWithURL(ctx, qc.queueURL, m, processErr, &stats)
			}(msg)
		}

		wg.Wait()

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

	// Change visibility timeout based on event type configuration (before processing starts)
	if timeout, exists := c.config.GetEventTimeout(eventType); exists {
		if err := c.changeVisibilityTimeoutWithURL(ctx, queueURL, receiptHandle, timeout); err != nil {
			c.logger.Warn().
				Str("event_type", eventType).
				Int("timeout", timeout).
				Err(err).
				Msg("Failed to change visibility timeout for event")
		} else {
			c.logger.Debug().
				Str("event_type", eventType).
				Int("timeout", timeout).
				Msg("Changed visibility timeout for event")
		}
	} else if c.config.IsLongRunningEvent(eventType) {
		// Fallback to legacy long-running events (5 minutes)
		c.changeVisibilityTimeoutWithURL(ctx, queueURL, receiptHandle, 300)
	}

	// Get handler
	handler, ok := c.eventRegistry.GetHandler(eventType)
	if !ok {
		return NewPermanentError("no handler registered for event type: "+eventType, nil)
	}

	// Create context with message metadata
	handlerCtx := context.WithValue(ctx, ContextKeySourceService, env.Service)
	handlerCtx = context.WithValue(handlerCtx, ContextKeyEventType, eventType)
	handlerCtx = context.WithValue(handlerCtx, ContextKeyTraceID, env.GetTraceID())
	handlerCtx = context.WithValue(handlerCtx, ContextKeyMessageID, msg.MessageID)
	handlerCtx = context.WithValue(handlerCtx, ContextKeyQueueName, queueURL)

	// Execute handler
	startTime := time.Now()
	if err := handler(handlerCtx, env.Payload); err != nil {
		// Classify the error
		if isTransientErr(err) {
			return NewTransientError("handler failed", err)
		}
		return NewPermanentError("handler failed", err)
	}
	duration := time.Since(startTime)

	// Record metrics using unified provider
	c.metricsProvider.ObserveProcessingDuration(ctx, queueURL, eventType, float64(duration.Milliseconds()))
	c.metricsProvider.IncMessagesSuccess(ctx, queueURL, eventType)

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
		stats.mu.Lock()
		stats.success++
		stats.mu.Unlock()
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
		stats.mu.Lock()
		stats.validationErrors++
		stats.mu.Unlock()
		c.logger.Warn().
			Str("message_id", messageID).
			Err(err).
			Msg("Validation error, deleting message")
		c.deleteMessageWithURL(ctx, queueURL, receiptHandle)
		c.metricsProvider.IncValidationErrors(ctx, stats.queueName, "")

	case ErrorTypeTransient:
		stats.mu.Lock()
		stats.transientErrors++
		stats.mu.Unlock()
		c.logger.Warn().
			Str("message_id", messageID).
			Err(err).
			Msg("Transient error, leaving for retry")
		// Don't delete - will be retried or sent to DLQ
		c.metricsProvider.IncTransientErrors(ctx, stats.queueName, "")

	case ErrorTypePermanent:
		stats.mu.Lock()
		stats.permanentErrors++
		stats.mu.Unlock()
		c.logger.Error().
			Str("message_id", messageID).
			Err(err).
			Msg("Permanent error, deleting message")
		c.deleteMessageWithURL(ctx, queueURL, receiptHandle)
		c.metricsProvider.IncPermanentErrors(ctx, stats.queueName, "")

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
