package service

import (
	"encoding/json"

	"github.com/Go-routine-4595/DataEnricher/domain"
)

type IProcessMessage interface {
	ProcessMessage(msg []byte) (domain.EnrichedMessage, error)
}
type Service struct {
}

func NewService() *Service {
	return &Service{}
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

	return enrichedMessage, nil
}
