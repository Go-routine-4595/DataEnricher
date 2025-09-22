package usecase

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/Go-routine-4595/DataEnricher/service"
	"github.com/rs/zerolog"
)

type IDynatraceClient interface {
	RecordMessageProcessed(topic string, processingTimeMs float64, success bool)
	RecordError(errorType, topic string)
}

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
	dynatrace        IDynatraceClient
}

func NewUseCase(pub IPublishMessage, srv service.IProcessMessage, dynatrace IDynatraceClient, publishTopic string, l *zerolog.Logger, ctx context.Context) *UseCase {
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
		dynatrace:        dynatrace,
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
			u.processMessage(msg)
		}
	}
}

func (u *UseCase) processMessage(msg []byte) {
	var (
		topic   string
		success bool
	)

	defer func(now time.Time) {
		elapsed := time.Since(now).Seconds()
		u.logger.Info().Msgf("ProcessMessage took %f second", elapsed)
		if success {
			//u.dynatrace.RecordMessageProcessed(u.topic, elapsed, true)
		} else {
			//u.dynatrace.RecordMessageProcessed(u.topic, elapsed, false)
			//s.dynatrace.RecordError("invalid_format", topic)
			// or
			// s.dynatrace.RecordError("enrichment_error", topic)
			// or
			//s.dynatrace.RecordError("repository_error", topic)
		}

	}(time.Now())

	enrichedMsg, err := u.srv.ProcessMessage(msg)
	if err != nil {
		var geoKonErr *service.ErrNotGeoKonAPIData
		if errors.As(err, &geoKonErr) {
			u.logger.Warn().Msgf("Invalid GeoKonAPI data: %v", err)
			u.logger.Debug().Msgf("Message: %s", string(msg))
			return
		}
		u.logger.Error().Msgf("Error processing message: %v", err)
		u.logger.Debug().Msgf("Message: %s", string(msg))
		return
	}
	b, err := enrichedMsg.Byte()
	if err != nil {
		u.logger.Error().Msgf("Error converting message to byte: %v", err)
		u.logger.Debug().Msgf("Message: %s", string(msg))
		return
	}
	topic = u.publishBaseTopic + "/" + enrichedMsg.SiteCode + "/" + enrichedMsg.DeviceID
	success = true
	if u.publishMessage != nil {
		u.publishMessage.PublishMessage(b, topic)
	} else {
		u.mockPublishMessage(b, topic)
	}
}

func (u *UseCase) mockPublishMessage(message []byte, topic string) {
	u.logger.Debug().Msgf("Publishing message to %s    msg: %s", topic, string(message))
}
