package service

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/Go-routine-4595/DataEnricher/domain"
	"github.com/rs/zerolog"
)

// Define custom error types
type ErrNotGeoKonAPIData struct {
	Message string
}

func (e *ErrNotGeoKonAPIData) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return "not a GeoKonAPI data model"
}

// Factory function for the error
func NewErrNotGeoKonAPIData(message string) *ErrNotGeoKonAPIData {
	return &ErrNotGeoKonAPIData{Message: message}
}

type IProcessMessage interface {
	ProcessMessage(msg []byte) (domain.EnrichedMessage, error)
}

type IRepository interface {
	Get(key string) (string, error)
}

type Service struct {
	repository IRepository
	logger     *zerolog.Logger
}

func NewService(repo IRepository, logger *zerolog.Logger) *Service {

	var l zerolog.Logger

	if logger == nil {
		l = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		l = *logger
	}
	return &Service{repository: repo, logger: &l}
}

func (s *Service) ProcessMessage(msg []byte) (domain.EnrichedMessage, error) {
	var (
		enrichedMessage domain.EnrichedMessage
		err             error
	)

	err = json.Unmarshal(msg, &enrichedMessage)
	if err != nil {
		return domain.EnrichedMessage{}, err
	}
	key := "device-" + enrichedMessage.DeviceID
	registry, err := s.repository.Get(key)
	if err != nil {
		return domain.EnrichedMessage{}, fmt.Errorf(" %w -- srource_topic: %s ", err, enrichedMessage.SourceTopic)
	}
	err = enrichedMessage.Enrich([]byte(registry))
	if err != nil {
		return domain.EnrichedMessage{}, err
	}

	if !enrichedMessage.IsGeoKonAPIDataModel() {
		return domain.EnrichedMessage{}, NewErrNotGeoKonAPIData("invalid data format")
	}

	return enrichedMessage, nil
}
