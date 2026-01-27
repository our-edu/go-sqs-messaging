// Package metrics provides metrics integration for SQS messaging
package metrics

import (
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/our-edu/go-sqs-messaging/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// FactoryConfig holds configuration for creating metrics providers
type FactoryConfig struct {
	// CloudWatch configuration
	CloudWatchEnabled   bool
	CloudWatchNamespace string
	CloudWatchClient    *cloudwatch.Client

	// Prometheus configuration
	PrometheusEnabled   bool
	PrometheusNamespace string
	PrometheusSubsystem string
	PrometheusRegistry  prometheus.Registerer

	// Full config reference (optional)
	Config *config.Config

	// Logger
	Logger zerolog.Logger
}

// Factory creates metrics providers based on configuration
type Factory struct {
	config FactoryConfig
}

// NewFactory creates a new metrics factory
func NewFactory(cfg FactoryConfig) *Factory {
	return &Factory{config: cfg}
}

// NewFactoryFromConfig creates a factory from the application config
func NewFactoryFromConfig(cfg *config.Config, cwClient *cloudwatch.Client, logger zerolog.Logger) *Factory {
	return &Factory{
		config: FactoryConfig{
			CloudWatchEnabled:   cfg.SQS.CloudWatch.Enabled,
			CloudWatchNamespace: cfg.SQS.CloudWatch.Namespace,
			CloudWatchClient:    cwClient,
			PrometheusEnabled:   cfg.SQS.Prometheus.Enabled,
			PrometheusNamespace: cfg.SQS.Prometheus.Namespace,
			PrometheusSubsystem: cfg.SQS.Prometheus.Subsystem,
			Config:              cfg,
			Logger:              logger,
		},
	}
}

// Create creates a metrics provider based on the factory configuration.
// If both CloudWatch and Prometheus are enabled, returns a CompositeProvider.
// If only one is enabled, returns that specific provider.
// If neither is enabled, returns a NoopProvider.
func (f *Factory) Create() Provider {
	var providers []Provider

	// Create CloudWatch provider if enabled
	if f.config.CloudWatchEnabled && f.config.CloudWatchClient != nil {
		cwProvider := NewCloudWatchProvider(
			f.config.CloudWatchClient,
			f.config.Config,
			f.config.Logger,
		)
		providers = append(providers, cwProvider)
		f.config.Logger.Debug().Msg("CloudWatch metrics provider created")
	}

	// Create Prometheus provider if enabled
	if f.config.PrometheusEnabled {
		promConfig := PrometheusConfig{
			Enabled:   true,
			Namespace: f.config.PrometheusNamespace,
			Subsystem: f.config.PrometheusSubsystem,
			Registry:  f.config.PrometheusRegistry,
		}
		promProvider := NewPrometheusProvider(f.config.Logger, promConfig)
		providers = append(providers, promProvider)
		f.config.Logger.Debug().Msg("Prometheus metrics provider created")
	}

	// Return appropriate provider based on what's enabled
	switch len(providers) {
	case 0:
		f.config.Logger.Debug().Msg("No metrics providers enabled, using NoopProvider")
		return NewNoopProvider()
	case 1:
		return providers[0]
	default:
		f.config.Logger.Debug().
			Int("provider_count", len(providers)).
			Msg("Multiple metrics providers enabled, using CompositeProvider")
		return NewCompositeProvider(providers...)
	}
}

// CreateCloudWatch creates only a CloudWatch provider
func (f *Factory) CreateCloudWatch() *CloudWatchProvider {
	if !f.config.CloudWatchEnabled || f.config.CloudWatchClient == nil {
		return nil
	}
	return NewCloudWatchProvider(
		f.config.CloudWatchClient,
		f.config.Config,
		f.config.Logger,
	)
}

// CreatePrometheus creates only a Prometheus provider
func (f *Factory) CreatePrometheus() *PrometheusProvider {
	if !f.config.PrometheusEnabled {
		return nil
	}
	promConfig := PrometheusConfig{
		Enabled:   true,
		Namespace: f.config.PrometheusNamespace,
		Subsystem: f.config.PrometheusSubsystem,
		Registry:  f.config.PrometheusRegistry,
	}
	return NewPrometheusProvider(f.config.Logger, promConfig)
}

// CreateNoop creates a NoopProvider
func (f *Factory) CreateNoop() *NoopProvider {
	return NewNoopProvider()
}

// WithPrometheusRegistry sets a custom Prometheus registry
func (f *Factory) WithPrometheusRegistry(registry prometheus.Registerer) *Factory {
	f.config.PrometheusRegistry = registry
	return f
}

// WithCloudWatchClient sets the CloudWatch client
func (f *Factory) WithCloudWatchClient(client *cloudwatch.Client) *Factory {
	f.config.CloudWatchClient = client
	return f
}
