package gateways

import (
	"os"

	"github.com/Go-routine-4595/DataEnricher/internal/config"
	mqtt "github.com/Go-routine-4595/DataEnricher/internal/mqtt"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Publish struct {
	client *mqtt.MQTTConnector
	logger *zerolog.Logger
}

func NewPublish(config *config.Config, logger *zerolog.Logger) *Publish {
	var l zerolog.Logger
	if logger == nil {
		l = zerolog.New(os.Stdout).With().Timestamp().Logger()
		logger = &l
	} else {
		l = *logger
	}
	cfg := mqtt.NewMQTTConfig(config.Host, config.Port, "DataEnricher-controller-"+uuid.New().String())
	if config.Password != "" {
		cfg.Password = &config.Password
	}
	if config.User != "" {
		cfg.Username = &config.User
	}

	client := mqtt.NewMQTTConnector(cfg, &l)
	err := client.Connect()

	if err != nil {
		l.Fatal().Msgf("Failed to connect to MQTT broker: %v", err)
	}

	return &Publish{
		client: client,
		logger: &l,
	}
}

func (p *Publish) PublishMessage(message []byte, topic string) {
	err := p.client.Publish(topic, message)
	p.logger.Debug().Msgf("Published data: %s to %s", string(message), topic)
	if err != nil {
		p.logger.Error().Msgf("Failed to publish to %s: %v", topic, err)
		p.logger.Debug().Msgf("Message: %s", string(message))
	}
}

func (p *Publish) Close() {
	p.client.Stop()
}
