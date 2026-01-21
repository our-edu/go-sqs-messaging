package metrics

import (
	"context"
	"testing"
)

func TestNoopProvider(t *testing.T) {
	p := NewNoopProvider()

	if p.Name() != string(ProviderTypeNoop) {
		t.Errorf("expected name %s, got %s", ProviderTypeNoop, p.Name())
	}

	if p.Enabled() {
		t.Error("expected Enabled() to return false")
	}

	ctx := context.Background()

	// All these should be no-ops and not panic
	_ = p.PutMetric(ctx, "test", 1.0, "Count", nil)
	_ = p.Increment(ctx, "test", nil)
	_ = p.RecordDuration(ctx, "test", 100.0, nil)
	p.IncMessagesProcessed(ctx, "queue", "event", "status")
	p.IncMessagesSuccess(ctx, "queue", "event")
	p.IncValidationErrors(ctx, "queue", "event")
	p.IncTransientErrors(ctx, "queue", "event")
	p.IncPermanentErrors(ctx, "queue", "event")
	p.IncMessagesPublished(ctx, "queue", "event")
	p.IncPublishErrors(ctx, "queue", "event")
	p.ObserveProcessingDuration(ctx, "queue", "event", 100.0)
	p.ObservePublishDuration(ctx, "queue", "event", 50.0)
	p.SetQueueDepth(ctx, "queue", 10)
	p.SetDLQDepth(ctx, "queue", 5)
	p.SetActiveConsumers(ctx, "queue", 3)
	p.IncActiveConsumers(ctx, "queue")
	p.DecActiveConsumers(ctx, "queue")
}

func TestCompositeProvider_Empty(t *testing.T) {
	p := NewCompositeProvider()

	if p.Name() != string(ProviderTypeComposite) {
		t.Errorf("expected name %s, got %s", ProviderTypeComposite, p.Name())
	}

	if p.Enabled() {
		t.Error("expected Enabled() to return false for empty composite")
	}

	if len(p.Providers()) != 0 {
		t.Errorf("expected 0 providers, got %d", len(p.Providers()))
	}
}

func TestCompositeProvider_WithProviders(t *testing.T) {
	noop1 := NewNoopProvider()
	noop2 := NewNoopProvider()

	// NoopProviders report as disabled, so they won't be added
	p := NewCompositeProvider(noop1, noop2)

	// Since NoopProviders are disabled, they won't be included
	if p.Enabled() {
		t.Error("expected Enabled() to return false when only noop providers")
	}
}

func TestCompositeProvider_NilHandling(t *testing.T) {
	// Should not panic with nil providers
	p := NewCompositeProvider(nil, nil)

	if p.Enabled() {
		t.Error("expected Enabled() to return false with nil providers")
	}

	// Handler and Collectors should return nil
	if p.Handler() != nil {
		t.Error("expected Handler() to return nil")
	}

	if p.Collectors() != nil && len(p.Collectors()) != 0 {
		t.Error("expected Collectors() to return empty slice")
	}
}

func TestProviderInterface(t *testing.T) {
	// Test that all providers implement the Provider interface
	var _ Provider = (*NoopProvider)(nil)
	var _ Provider = (*CompositeProvider)(nil)
	var _ Provider = (*CloudWatchProvider)(nil)
	var _ Provider = (*PrometheusProvider)(nil)

	// Test that appropriate providers implement HTTPProvider
	var _ HTTPProvider = (*CompositeProvider)(nil)
	var _ HTTPProvider = (*PrometheusProvider)(nil)

	// Test that appropriate providers implement CollectorProvider
	var _ CollectorProvider = (*CompositeProvider)(nil)
	var _ CollectorProvider = (*PrometheusProvider)(nil)
}
