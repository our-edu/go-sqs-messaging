// Package metrics provides metrics integration for SQS messaging
package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

// CompositeProvider aggregates multiple metrics providers and delegates calls to all of them.
// This allows sending metrics to multiple backends simultaneously (e.g., CloudWatch and Prometheus).
type CompositeProvider struct {
	providers []Provider
}

// NewCompositeProvider creates a new composite provider with the given providers.
// Only enabled providers are included.
func NewCompositeProvider(providers ...Provider) *CompositeProvider {
	enabledProviders := make([]Provider, 0, len(providers))
	for _, p := range providers {
		if p != nil && p.Enabled() {
			enabledProviders = append(enabledProviders, p)
		}
	}
	return &CompositeProvider{
		providers: enabledProviders,
	}
}

// Ensure CompositeProvider implements Provider, HTTPProvider, and CollectorProvider interfaces
var _ Provider = (*CompositeProvider)(nil)
var _ HTTPProvider = (*CompositeProvider)(nil)
var _ CollectorProvider = (*CompositeProvider)(nil)

// Name returns the provider name
func (c *CompositeProvider) Name() string {
	return string(ProviderTypeComposite)
}

// Enabled returns true if at least one provider is enabled
func (c *CompositeProvider) Enabled() bool {
	return len(c.providers) > 0
}

// PutMetric sends a metric to all providers
func (c *CompositeProvider) PutMetric(ctx context.Context, name string, value float64, unit string, dimensions map[string]string) error {
	var lastErr error
	for _, p := range c.providers {
		if err := p.PutMetric(ctx, name, value, unit, dimensions); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Increment increments a counter on all providers
func (c *CompositeProvider) Increment(ctx context.Context, name string, dimensions map[string]string) error {
	var lastErr error
	for _, p := range c.providers {
		if err := p.Increment(ctx, name, dimensions); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// RecordDuration records duration on all providers
func (c *CompositeProvider) RecordDuration(ctx context.Context, name string, duration float64, dimensions map[string]string) error {
	var lastErr error
	for _, p := range c.providers {
		if err := p.RecordDuration(ctx, name, duration, dimensions); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// IncMessagesProcessed increments messages processed counter on all providers
func (c *CompositeProvider) IncMessagesProcessed(ctx context.Context, queue, eventType, status string) {
	for _, p := range c.providers {
		p.IncMessagesProcessed(ctx, queue, eventType, status)
	}
}

// IncMessagesSuccess increments success counter on all providers
func (c *CompositeProvider) IncMessagesSuccess(ctx context.Context, queue, eventType string) {
	for _, p := range c.providers {
		p.IncMessagesSuccess(ctx, queue, eventType)
	}
}

// IncValidationErrors increments validation errors on all providers
func (c *CompositeProvider) IncValidationErrors(ctx context.Context, queue, eventType string) {
	for _, p := range c.providers {
		p.IncValidationErrors(ctx, queue, eventType)
	}
}

// IncTransientErrors increments transient errors on all providers
func (c *CompositeProvider) IncTransientErrors(ctx context.Context, queue, eventType string) {
	for _, p := range c.providers {
		p.IncTransientErrors(ctx, queue, eventType)
	}
}

// IncPermanentErrors increments permanent errors on all providers
func (c *CompositeProvider) IncPermanentErrors(ctx context.Context, queue, eventType string) {
	for _, p := range c.providers {
		p.IncPermanentErrors(ctx, queue, eventType)
	}
}

// IncMessagesPublished increments published counter on all providers
func (c *CompositeProvider) IncMessagesPublished(ctx context.Context, queue, eventType string) {
	for _, p := range c.providers {
		p.IncMessagesPublished(ctx, queue, eventType)
	}
}

// IncPublishErrors increments publish errors on all providers
func (c *CompositeProvider) IncPublishErrors(ctx context.Context, queue, eventType string) {
	for _, p := range c.providers {
		p.IncPublishErrors(ctx, queue, eventType)
	}
}

// ObserveProcessingDuration records processing duration on all providers
func (c *CompositeProvider) ObserveProcessingDuration(ctx context.Context, queue, eventType string, durationMs float64) {
	for _, p := range c.providers {
		p.ObserveProcessingDuration(ctx, queue, eventType, durationMs)
	}
}

// ObservePublishDuration records publish duration on all providers
func (c *CompositeProvider) ObservePublishDuration(ctx context.Context, queue, eventType string, durationMs float64) {
	for _, p := range c.providers {
		p.ObservePublishDuration(ctx, queue, eventType, durationMs)
	}
}

// SetQueueDepth sets queue depth on all providers
func (c *CompositeProvider) SetQueueDepth(ctx context.Context, queue string, depth float64) {
	for _, p := range c.providers {
		p.SetQueueDepth(ctx, queue, depth)
	}
}

// SetDLQDepth sets DLQ depth on all providers
func (c *CompositeProvider) SetDLQDepth(ctx context.Context, queue string, depth float64) {
	for _, p := range c.providers {
		p.SetDLQDepth(ctx, queue, depth)
	}
}

// SetActiveConsumers sets active consumers count on all providers
func (c *CompositeProvider) SetActiveConsumers(ctx context.Context, queue string, count float64) {
	for _, p := range c.providers {
		p.SetActiveConsumers(ctx, queue, count)
	}
}

// IncActiveConsumers increments active consumers on all providers
func (c *CompositeProvider) IncActiveConsumers(ctx context.Context, queue string) {
	for _, p := range c.providers {
		p.IncActiveConsumers(ctx, queue)
	}
}

// DecActiveConsumers decrements active consumers on all providers
func (c *CompositeProvider) DecActiveConsumers(ctx context.Context, queue string) {
	for _, p := range c.providers {
		p.DecActiveConsumers(ctx, queue)
	}
}

// Handler returns the HTTP handler from the first HTTPProvider found.
// Returns nil if no HTTPProvider is available.
func (c *CompositeProvider) Handler() http.Handler {
	for _, p := range c.providers {
		if hp, ok := p.(HTTPProvider); ok {
			return hp.Handler()
		}
	}
	return nil
}

// HandlerFunc returns the HTTP handler func from the first HTTPProvider found.
// Returns nil if no HTTPProvider is available.
func (c *CompositeProvider) HandlerFunc() http.HandlerFunc {
	for _, p := range c.providers {
		if hp, ok := p.(HTTPProvider); ok {
			return hp.HandlerFunc()
		}
	}
	return nil
}

// Collectors returns all Prometheus collectors from all CollectorProviders.
func (c *CompositeProvider) Collectors() []prometheus.Collector {
	var collectors []prometheus.Collector
	for _, p := range c.providers {
		if cp, ok := p.(CollectorProvider); ok {
			collectors = append(collectors, cp.Collectors()...)
		}
	}
	return collectors
}

// Register registers all CollectorProviders.
func (c *CompositeProvider) Register() error {
	var lastErr error
	for _, p := range c.providers {
		if cp, ok := p.(CollectorProvider); ok {
			if err := cp.Register(); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

// GetProvider returns the first provider of the given type, or nil if not found.
func (c *CompositeProvider) GetProvider(providerType ProviderType) Provider {
	for _, p := range c.providers {
		if p.Name() == string(providerType) {
			return p
		}
	}
	return nil
}

// GetPrometheusProvider returns the Prometheus provider if available.
func (c *CompositeProvider) GetPrometheusProvider() *PrometheusProvider {
	for _, p := range c.providers {
		if pp, ok := p.(*PrometheusProvider); ok {
			return pp
		}
	}
	return nil
}

// Providers returns all underlying providers.
func (c *CompositeProvider) Providers() []Provider {
	return c.providers
}
