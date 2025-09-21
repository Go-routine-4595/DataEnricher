package config

import (
	"os"
	"strconv"
)

type Config struct {
	Host              string
	Port              int
	LogLevel          string
	SubscriptionTopic string
	PublishTopicBase  string
	User              string
	Password          string
	LogFilePath       string
}

func Load() *Config {
	port, _ := strconv.Atoi(getEnvOrDefault("PORT", "8883"))

	return &Config{
		Host:              getEnvOrDefault("HOST", "backend.christophe.engineering"),
		Port:              port,
		LogLevel:          getEnvOrDefault("LOG_LEVEL", "debug"),
		SubscriptionTopic: getEnvOrDefault("SUBSCRIPTION_TOPIC", "FCTS/INGRESS/ENRICH"),
		PublishTopicBase:  getEnvOrDefault("PUBLISH_TOPIC_BASE", "FCTS/ENRICHED/geokonapi"),
		User:              getEnvOrDefault("USER", ""),
		Password:          getEnvOrDefault("PASSWORD", ""),
		LogFilePath:       getEnvOrDefault("LOG_FILE_PATH", "logs"),
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
