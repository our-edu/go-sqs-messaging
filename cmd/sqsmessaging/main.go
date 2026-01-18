// Package main provides the CLI entry point for the SQS messaging system
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	"github.com/our-edu/go-sqs-messaging/internal/config"
)

var (
	cfg    *config.Config
	logger zerolog.Logger
)

func main() {
	// Initialize logger
	logger = zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Load configuration from .env file and environment variables
	cfg = config.Load()
	logger.Debug().
		Str("driver", string(cfg.Messaging.Driver)).
		Str("region", cfg.AWS.Region).
		Str("prefix", cfg.SQS.Prefix).
		Msg("Configuration loaded")

	// Create root command
	rootCmd := &cobra.Command{
		Use:   "sqsmessaging",
		Short: "SQS Messaging CLI - A Go port of Laravel SQS Messaging",
		Long: `SQS Messaging CLI provides commands for managing AWS SQS message queues,
consuming messages, monitoring DLQs, and more.

This is a Go port of the Laravel our-edu/laravel-sqs-messaging package.`,
	}

	// Add subcommands
	rootCmd.AddCommand(
		newConsumeCmd(),
		newEnsureCmd(),
		newStatusCmd(),
		newCleanupCmd(),
		newInspectDlqCmd(),
		newMonitorDlqCmd(),
		newReplayDlqCmd(),
		newTestConnectionCmd(),
		newTestReceiveCmd(),
	)

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info().Msg("Received shutdown signal")
		cancel()
	}()

	// Execute
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		logger.Error().Err(err).Msg("Command failed")
		os.Exit(1)
	}
}
