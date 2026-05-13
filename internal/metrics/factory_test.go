package metrics

import (
	"context"
	"testing"

	"github.com/our-edu/go-sqs-messaging/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// ----- Factory.Create() -----

func TestFactory_Create_NeitherEnabled_ReturnsNoop(t *testing.T) {
	f := NewFactory(FactoryConfig{
		CloudWatchEnabled: false,
		PrometheusEnabled: false,
		Logger:            zerolog.Nop(),
	})

	provider := f.Create()
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
	if provider.Name() != string(ProviderTypeNoop) {
		t.Errorf("expected NoopProvider, got %s", provider.Name())
	}
	if provider.Enabled() {
		t.Error("expected Noop provider to be disabled")
	}
}

func TestFactory_Create_PrometheusOnly_ReturnsPrometheusProvider(t *testing.T) {
	reg := prometheus.NewRegistry()
	f := NewFactory(FactoryConfig{
		CloudWatchEnabled:   false,
		PrometheusEnabled:   true,
		PrometheusNamespace: "fact_test",
		PrometheusRegistry:  reg,
		Logger:              zerolog.Nop(),
	})

	provider := f.Create()
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
	if provider.Name() != string(ProviderTypePrometheus) {
		t.Errorf("expected PrometheusProvider, got %s", provider.Name())
	}
	if !provider.Enabled() {
		t.Error("expected PrometheusProvider to be enabled")
	}
}

func TestFactory_Create_CloudWatchNoClient_FallsBackToNoop(t *testing.T) {
	// CloudWatch enabled but nil client → factory skips it → noop
	f := NewFactory(FactoryConfig{
		CloudWatchEnabled: true,
		CloudWatchClient:  nil,
		PrometheusEnabled: false,
		Logger:            zerolog.Nop(),
	})

	provider := f.Create()
	if provider.Name() != string(ProviderTypeNoop) {
		t.Errorf("expected NoopProvider when CW client is nil, got %s", provider.Name())
	}
}

func TestFactory_Create_BothEnabled_NoCloudWatchClient_ReturnsPrometheus(t *testing.T) {
	// CW enabled but client is nil → only Prometheus is created → single, not composite
	reg := prometheus.NewRegistry()
	f := NewFactory(FactoryConfig{
		CloudWatchEnabled:   true,
		CloudWatchClient:    nil,
		PrometheusEnabled:   true,
		PrometheusNamespace: "fact_both_test",
		PrometheusRegistry:  reg,
		Logger:              zerolog.Nop(),
	})

	provider := f.Create()
	if provider.Name() != string(ProviderTypePrometheus) {
		t.Errorf("expected PrometheusProvider (CW client nil), got %s", provider.Name())
	}
}

// ----- Factory convenience constructors -----

func TestFactory_CreateNoop(t *testing.T) {
	f := NewFactory(FactoryConfig{Logger: zerolog.Nop()})

	noop := f.CreateNoop()
	if noop == nil {
		t.Fatal("expected non-nil NoopProvider")
	}
	if noop.Name() != string(ProviderTypeNoop) {
		t.Errorf("expected NoopProvider, got %s", noop.Name())
	}
}

func TestFactory_CreatePrometheus_DisabledReturnsNil(t *testing.T) {
	f := NewFactory(FactoryConfig{
		PrometheusEnabled: false,
		Logger:            zerolog.Nop(),
	})

	if p := f.CreatePrometheus(); p != nil {
		t.Error("expected nil when Prometheus is disabled")
	}
}

func TestFactory_CreatePrometheus_EnabledReturnsProvider(t *testing.T) {
	reg := prometheus.NewRegistry()
	f := NewFactory(FactoryConfig{
		PrometheusEnabled:   true,
		PrometheusNamespace: "fact_prom_test",
		PrometheusRegistry:  reg,
		Logger:              zerolog.Nop(),
	})

	p := f.CreatePrometheus()
	if p == nil {
		t.Fatal("expected non-nil PrometheusProvider")
	}
	if p.Name() != string(ProviderTypePrometheus) {
		t.Errorf("expected PrometheusProvider, got %s", p.Name())
	}
}

func TestFactory_CreateCloudWatch_NilClientReturnsNil(t *testing.T) {
	f := NewFactory(FactoryConfig{
		CloudWatchEnabled: true,
		CloudWatchClient:  nil,
		Logger:            zerolog.Nop(),
	})

	if cw := f.CreateCloudWatch(); cw != nil {
		t.Error("expected nil when CloudWatch client is nil")
	}
}

func TestFactory_CreateCloudWatch_DisabledReturnsNil(t *testing.T) {
	f := NewFactory(FactoryConfig{
		CloudWatchEnabled: false,
		Logger:            zerolog.Nop(),
	})

	if cw := f.CreateCloudWatch(); cw != nil {
		t.Error("expected nil when CloudWatch is disabled")
	}
}

// ----- Factory fluent setters -----

func TestFactory_WithPrometheusRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	f := NewFactory(FactoryConfig{Logger: zerolog.Nop()}).
		WithPrometheusRegistry(reg)

	if f.config.PrometheusRegistry != reg {
		t.Error("expected registry to be updated by WithPrometheusRegistry")
	}
}

// ----- NewFactoryFromConfig -----

func TestNewFactoryFromConfig_DefaultConfigGivesNoop(t *testing.T) {
	cfg := config.DefaultConfig()
	f := NewFactoryFromConfig(cfg, nil, zerolog.Nop())
	if f == nil {
		t.Fatal("expected non-nil factory")
	}

	// Default config has no metrics enabled
	provider := f.Create()
	if provider.Name() != string(ProviderTypeNoop) {
		t.Errorf("expected NoopProvider from default config, got %s", provider.Name())
	}
}

// ----- PrometheusProvider -----

func TestPrometheusProvider_Name(t *testing.T) {
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{
		Enabled:  true,
		Registry: prometheus.NewRegistry(),
	})
	if p.Name() != string(ProviderTypePrometheus) {
		t.Errorf("expected name %s, got %s", ProviderTypePrometheus, p.Name())
	}
}

func TestPrometheusProvider_Enabled_TrueWhenConfigured(t *testing.T) {
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{
		Enabled:  true,
		Registry: prometheus.NewRegistry(),
	})
	if !p.Enabled() {
		t.Error("expected Enabled() = true")
	}
}

func TestPrometheusProvider_Enabled_FalseWhenDisabled(t *testing.T) {
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{Enabled: false})
	if p.Enabled() {
		t.Error("expected Enabled() = false for disabled provider")
	}
}

func TestPrometheusProvider_DefaultNamespace(t *testing.T) {
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{
		Enabled:   true,
		Namespace: "", // empty → should default to "sqsmessaging"
		Registry:  prometheus.NewRegistry(),
	})
	if p.namespace != "sqsmessaging" {
		t.Errorf("expected namespace 'sqsmessaging', got '%s'", p.namespace)
	}
}

func TestPrometheusProvider_Collectors_Returns12(t *testing.T) {
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{
		Enabled:  true,
		Registry: prometheus.NewRegistry(),
	})
	collectors := p.Collectors()
	if len(collectors) != 12 {
		t.Errorf("expected 12 collectors, got %d", len(collectors))
	}
}

func TestPrometheusProvider_Register_Idempotent(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{
		Enabled:   true,
		Namespace: "idem_test",
		Registry:  reg,
	})

	if err := p.Register(); err != nil {
		t.Fatalf("first Register() failed: %v", err)
	}
	// Second call must be a no-op, not an error
	if err := p.Register(); err != nil {
		t.Errorf("second Register() returned error: %v", err)
	}
}

func TestPrometheusProvider_AllMetricMethods_NoPanic(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{
		Enabled:   true,
		Namespace: "nopanic_test",
		Registry:  reg,
	})

	ctx := context.Background()
	p.IncMessagesProcessed(ctx, "queue", "event", "success")
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

func TestPrometheusProvider_DisabledProvider_AllMethodsNoop(t *testing.T) {
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{Enabled: false})

	ctx := context.Background()
	// None of these should panic or error
	p.IncMessagesSuccess(ctx, "q", "e")
	p.IncValidationErrors(ctx, "q", "e")
	p.ObserveProcessingDuration(ctx, "q", "e", 42.0)
	p.SetQueueDepth(ctx, "q", 99)

	if err := p.PutMetric(ctx, "test", 1.0, "Count", nil); err != nil {
		t.Errorf("PutMetric on disabled provider returned error: %v", err)
	}
	if err := p.Increment(ctx, "test", nil); err != nil {
		t.Errorf("Increment on disabled provider returned error: %v", err)
	}
	if err := p.RecordDuration(ctx, "test", 10.0, nil); err != nil {
		t.Errorf("RecordDuration on disabled provider returned error: %v", err)
	}
}

func TestPrometheusProvider_Handler_ReturnsNonNil(t *testing.T) {
	// default registerer path
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{Enabled: true})
	if p.Handler() == nil {
		t.Error("expected non-nil Handler()")
	}
	if p.HandlerFunc() == nil {
		t.Error("expected non-nil HandlerFunc()")
	}
}

func TestPrometheusProvider_Handler_CustomRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{
		Enabled:  true,
		Registry: reg,
	})
	if p.Handler() == nil {
		t.Error("expected non-nil Handler() with custom registry")
	}
}

func TestPrometheusProvider_CounterIncrements_Verifiable(t *testing.T) {
	reg := prometheus.NewRegistry()
	p := NewPrometheusProvider(zerolog.Nop(), PrometheusConfig{
		Enabled:   true,
		Namespace: "verify_test",
		Registry:  reg,
	})
	if err := p.Register(); err != nil {
		t.Fatalf("Register() failed: %v", err)
	}

	ctx := context.Background()
	p.IncMessagesSuccess(ctx, "my-queue", "OrderCreated")
	p.IncMessagesSuccess(ctx, "my-queue", "OrderCreated")
	p.IncValidationErrors(ctx, "my-queue", "OrderCreated")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() failed: %v", err)
	}

	counts := make(map[string]float64)
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			counts[mf.GetName()] += m.GetCounter().GetValue()
		}
	}

	if v := counts["verify_test_messages_success_total"]; v != 2 {
		t.Errorf("expected messages_success_total=2, got %v", v)
	}
	if v := counts["verify_test_validation_errors_total"]; v != 1 {
		t.Errorf("expected validation_errors_total=1, got %v", v)
	}
}

func TestDefaultPrometheusConfig(t *testing.T) {
	cfg := DefaultPrometheusConfig()
	if !cfg.Enabled {
		t.Error("expected default config to be enabled")
	}
	if cfg.Namespace != "sqsmessaging" {
		t.Errorf("expected namespace 'sqsmessaging', got '%s'", cfg.Namespace)
	}
	if cfg.Registry != nil {
		t.Error("expected nil registry in default config")
	}
}
