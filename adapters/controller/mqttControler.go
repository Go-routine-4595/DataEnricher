package controller

import (
	"context"
	"os"

	"github.com/Go-routine-4595/DataEnricher/internal/config"
	"github.com/Go-routine-4595/DataEnricher/usecase"

	mqtt "github.com/Go-routine-4595/DataEnricher/internal/mqtt"
	"github.com/rs/zerolog"
)

type MqttController struct {
	controller *mqtt.MQTTConnector
	useCase    usecase.IGeoKonAPIMessage
	logger     *zerolog.Logger
}

func NewMqttController(config *config.Config, useCase usecase.IGeoKonAPIMessage, logger *zerolog.Logger) *MqttController {
	var (
		user  *string
		passw *string
		sub   *string
	)

	if config.User != "" {
		user = &config.User
	} else {
		user = nil
	}
	if config.Password != "" {
		passw = &config.Password
	} else {
		passw = nil
	}
	if config.SubscriptionTopic != "" {
		sub = &config.SubscriptionTopic
	} else {
		sub = nil
	}
	cfg := &mqtt.MQTTConfig{
		Host:           config.Host,
		Port:           config.Port,
		Keepalive:      60,
		Username:       user,
		Password:       passw,
		SubscribeTopic: sub,
		ClientID:       "DataEnricher",
	}
	controller := mqtt.NewMQTTConnector(cfg, logger).WithLogger(logger).WithSubscription(useCase)

	var l zerolog.Logger

	if logger == nil {
		l = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		l = *logger
	}

	return &MqttController{
		controller: controller,
		useCase:    useCase,
		logger:     &l,
	}
}

func (c *MqttController) onMessage(message mqtt.Message) {
	err := c.useCase.GeoKonAPIMessage(message.Payload())
	if err != nil {
		c.logger.Error().Msgf("Error processing message: %v message: %s", err, string(message.Payload()))
	}
}

func (c *MqttController) Start(ctx context.Context) error {
	var err error

	go func() {
		err = c.controller.Start(ctx)
	}()

	return err
}
