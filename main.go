package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/Go-routine-4595/DataEnricher/adapters/controller"
	"github.com/Go-routine-4595/DataEnricher/adapters/gateways"
	"github.com/Go-routine-4595/DataEnricher/internal/config"
	"github.com/Go-routine-4595/DataEnricher/internal/dynatrace"
	"github.com/Go-routine-4595/DataEnricher/service"
	"github.com/Go-routine-4595/DataEnricher/usecase"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Setup context for cancellation
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)

	// Setup logger
	logger := setupLogger(cfg.LogLevel, cfg.LogFilePath)

	// print config parameters
	printConfig(*cfg, &logger)

	// Setup Redis client
	redis, err := gateways.NewRepository(cfg.RedisConnectionString, &logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to Redis")
	}
	if redis.IsConnected() == false {
		logger.Fatal().Msg("Failed to connect to Redis")
	}
	logger.Info().Msg("Connected to Redis")
	defer redis.Close()

	// Setup Dynatrace client
	var dynatraceClient *dynatrace.DynatraceClient
	if cfg.DynatraceEnabled {
		dynatraceClient = dynatrace.NewDynatraceClient(&logger)
		logger.Info().Msg("Dynatrace metrics enabled")
	} else {
		dynatraceClient = dynatrace.NewDynatraceClient(&logger)
		dynatraceClient.Disable()
		logger.Info().Msg("Dynatrace metrics disabled")
	}

	// Setup publisher client
	pub := gateways.NewPublish(cfg, &logger)
	defer pub.Close()

	// Setup service and use case
	srv := service.NewService(redis, &logger)
	useCase := usecase.NewUseCase(pub, srv, dynatraceClient, cfg.PublishTopicBase, &logger, ctx)

	// Setup MQTT controller
	ctl := controller.NewMqttController(cfg, useCase, &logger)

	err = ctl.Start(ctx)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to start controller")
	}

	// Setup signal handler for graceful shutdown
	//sigChan := make(chan os.Signal, 1)
	//signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for signal
	//sig := <-sigChan
	<-ctx.Done()
	cancel()
	logger.Info().Msg("Received signal SIGINIT. Shutting down gracefully...")
}

func setupLogger(level string, logDir string) zerolog.Logger {
	// Create logs directory if it doesn't exist
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatal().Err(err).Msg("Failed to create log directory")
	}

	// Configure lumberjack for log rotation
	fileWriter := &lumberjack.Logger{
		Filename:   filepath.Join(logDir, "DataEnricher.log"),
		MaxSize:    4,  // megabytes
		MaxBackups: 5,  // number of backups
		MaxAge:     30, // days
		LocalTime:  true,
		Compress:   false, // compress rotated files
	}

	// Create multi-writer (console + file)
	multi := zerolog.MultiLevelWriter(
		zerolog.ConsoleWriter{Out: os.Stdout},
		fileWriter,
	)

	// Set global log level
	switch strings.ToLower(level) {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn", "warning":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}

	// Set global logger
	return zerolog.New(multi).With().Timestamp().Logger()
}

func printConfig(cfg config.Config, logger *zerolog.Logger) {
	logger.Info().Msg("Configuration:")
	logger.Info().Str("LOG_LEVEL", cfg.LogLevel).Msg("Log level")
	logger.Info().Str("HOST", cfg.Host).Msg("MQTT host")
	logger.Info().Int("PORT", cfg.Port).Msg("MQTT port")
	logger.Info().Str("REDIS_CONNECTION_STRING", cfg.RedisConnectionString).Msg("Redis connection string")
	logger.Info().Str("PUBLISH_TOPIC_BASE", cfg.PublishTopicBase).Msg("Publish topic base")
	logger.Info().Str("USER", cfg.User).Msg("MQTT user")
	logger.Info().Str("PASSWORD", cfg.Password).Msg("MQTT password")
	logger.Info().Str("LOG_FILE_PATH", cfg.LogFilePath).Msg("Log file path")
	logger.Info().Str("SUBSCRIPTION_TOPIC", cfg.SubscriptionTopic).Msg("Subscription topic")
	logger.Info().Bool("DYNATRACE_ENABLED", cfg.DynatraceEnabled).Msg("Dynatrace enabled")
}
