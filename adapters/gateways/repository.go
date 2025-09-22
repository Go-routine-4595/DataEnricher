package gateways

import (
	"os"

	"github.com/Go-routine-4595/DataEnricher/internal/redis"
	"github.com/rs/zerolog"
)

type Repository struct {
	redis  *redis.Client
	logger *zerolog.Logger
}

func NewRepository(connectionString string, logger *zerolog.Logger) (*Repository, error) {
	redis, err := redis.NewClientFromConnectionString(connectionString)
	if err != nil {
		return nil, err
	}

	var l zerolog.Logger

	if logger == nil {
		l = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		l = *logger
	}

	return &Repository{redis: redis, logger: &l}, nil
}

func (r *Repository) Close() {
	r.logger.Info().Msg("Closing Redis connection")
	r.redis.Close()
}

func (r *Repository) IsConnected() bool {
	return r.redis.IsConnected()
}

func (r *Repository) Get(key string) (string, error) {
	return r.redis.Get(key)
}
