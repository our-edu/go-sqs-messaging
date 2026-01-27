// Package main demonstrates how to extend your own Cobra CLI with SQS messaging commands.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	"github.com/our-edu/go-sqs-messaging/commands"
	"github.com/our-edu/go-sqs-messaging/pkg/config"
)

func main() {
	// Initialize logger (you can use your own logger)
	logger := zerolog.New(os.Stderr).With().Timestamp().Logger()

	// Load configuration (you can load your own config)
	cfg := config.Load()
	logger.Debug().
		Str("driver", string(cfg.Messaging.Driver)).
		Str("region", cfg.AWS.Region).
		Str("prefix", cfg.SQS.Prefix).
		Msg("Configuration loaded")

	// Create your own root command
	rootCmd := &cobra.Command{
		Use:   "myapp",
		Short: "My Application CLI",
		Long:  `My application with extended SQS messaging capabilities.`,
	}

	// Add your own commands here
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Show version",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Println("MyApp v1.0.0")
		},
	})

	// Add SQS messaging commands to your CLI
	commands.AddCommands(rootCmd, cfg, logger)

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
