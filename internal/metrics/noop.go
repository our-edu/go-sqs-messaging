// Package metrics provides metrics integration for SQS messaging
package metrics

import (
	"context"
)

// NoopProvider is a no-operation metrics provider that does nothing.
// Used when metrics are disabled or as a fallback.
type NoopProvider struct{}

// NewNoopProvider creates a new no-operation metrics provider
func NewNoopProvider() *NoopProvider {
	return &NoopProvider{}
}

// Ensure NoopProvider implements Provider interface
var _ Provider = (*NoopProvider)(nil)

// Name returns the provider name
func (n *NoopProvider) Name() string {
	return string(ProviderTypeNoop)
}

// Enabled returns false as this provider does nothing
func (n *NoopProvider) Enabled() bool {
	return false
}

// PutMetric does nothing
func (n *NoopProvider) PutMetric(ctx context.Context, name string, value float64, unit string, dimensions map[string]string) error {
	return nil
}

// Increment does nothing
func (n *NoopProvider) Increment(ctx context.Context, name string, dimensions map[string]string) error {
	return nil
}

// RecordDuration does nothing
func (n *NoopProvider) RecordDuration(ctx context.Context, name string, duration float64, dimensions map[string]string) error {
	return nil
}

// IncMessagesProcessed does nothing
func (n *NoopProvider) IncMessagesProcessed(ctx context.Context, queue, eventType, status string) {}

// IncMessagesSuccess does nothing
func (n *NoopProvider) IncMessagesSuccess(ctx context.Context, queue, eventType string) {}

// IncValidationErrors does nothing
func (n *NoopProvider) IncValidationErrors(ctx context.Context, queue, eventType string) {}

// IncTransientErrors does nothing
func (n *NoopProvider) IncTransientErrors(ctx context.Context, queue, eventType string) {}

// IncPermanentErrors does nothing
func (n *NoopProvider) IncPermanentErrors(ctx context.Context, queue, eventType string) {}

// IncMessagesPublished does nothing
func (n *NoopProvider) IncMessagesPublished(ctx context.Context, queue, eventType string) {}

// IncPublishErrors does nothing
func (n *NoopProvider) IncPublishErrors(ctx context.Context, queue, eventType string) {}

// ObserveProcessingDuration does nothing
func (n *NoopProvider) ObserveProcessingDuration(ctx context.Context, queue, eventType string, durationMs float64) {
}

// ObservePublishDuration does nothing
func (n *NoopProvider) ObservePublishDuration(ctx context.Context, queue, eventType string, durationMs float64) {
}

// SetQueueDepth does nothing
func (n *NoopProvider) SetQueueDepth(ctx context.Context, queue string, depth float64) {}

// SetDLQDepth does nothing
func (n *NoopProvider) SetDLQDepth(ctx context.Context, queue string, depth float64) {}

// SetActiveConsumers does nothing
func (n *NoopProvider) SetActiveConsumers(ctx context.Context, queue string, count float64) {}

// IncActiveConsumers does nothing
func (n *NoopProvider) IncActiveConsumers(ctx context.Context, queue string) {}

// DecActiveConsumers does nothing
func (n *NoopProvider) DecActiveConsumers(ctx context.Context, queue string) {}
