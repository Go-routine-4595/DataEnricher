package usecase

import (
	"context"
	"errors"
	"os"

	"github.com/Go-routine-4595/DataEnricher/service"
	"github.com/rs/zerolog"
)

type IGeoKonAPIMessage interface {
	GeoKonAPIMessage(message []byte) error
}
type IPublishMessage interface {
	PublishMessage(message []byte, topic string)
}
type UseCase struct {
	publishMessage   IPublishMessage
	srv              service.IProcessMessage
	logger           *zerolog.Logger
	channel          chan []byte
	publishBaseTopic string
}

func NewUseCase(pub IPublishMessage, srv service.IProcessMessage, publishTopic string, l *zerolog.Logger, ctx context.Context) *UseCase {
	var (
		logger zerolog.Logger
	)

	if l == nil {
		logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		logger = *l
	}

	useCase := &UseCase{
		publishMessage:   pub,
		srv:              srv,
		logger:           &logger,
		channel:          make(chan []byte, 100),
		publishBaseTopic: publishTopic,
	}

	go useCase.start(ctx)

	return useCase
}

func (u *UseCase) GeoKonAPIMessage(message []byte) error {

	select {
	case u.channel <- message:
		// Message was sent successfully
		u.logger.Debug().Msg("Message sent to channel successfully")
	default:
		// Channel is full, handle accordingly
		return errors.New("channel is full")
	}
	return nil
}

func (u *UseCase) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			u.logger.Info().Msg("UseCase Context done, exiting")
			return
		case msg := <-u.channel:
			enrichedMsg, err := u.srv.ProcessMessage(msg)
			if err != nil {
				u.logger.Error().Msgf("Error processing message: %v", err)
				u.logger.Debug().Msgf("Message: %s", string(msg))
			}
			b, err := enrichedMsg.Byte()
			if err != nil {
				u.logger.Error().Msgf("Error converting message to byte: %v", err)
				u.logger.Debug().Msgf("Message: %s", string(msg))
			}
			if u.publishMessage != nil {
				u.publishMessage.PublishMessage(b, u.publishBaseTopic)
			} else {
				u.mockPublishMessage(b, u.publishBaseTopic)
			}
		}
	}
}

func (u *UseCase) mockPublishMessage(message []byte, topic string) {
	u.logger.Debug().Msgf("Publishing message to %s    msg: %s", topic, string(message))
}
