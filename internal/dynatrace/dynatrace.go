package dynatrace

import (
	"fmt"
	"time"

	"github.com/dynatrace-oss/dynatrace-metric-utils-go/metric"
	"github.com/dynatrace-oss/dynatrace-metric-utils-go/metric/dimensions"
	"github.com/dynatrace-oss/dynatrace-metric-utils-go/oneagentenrichment"
	"github.com/rs/zerolog"
)

// DynatraceClient wraps Dynatrace metric functionality
type DynatraceClient struct {
	logger  *zerolog.Logger
	enabled bool
}

// NewDynatraceClient creates a new Dynatrace client
func NewDynatraceClient(logger *zerolog.Logger) *DynatraceClient {
	return &DynatraceClient{
		logger:  logger,
		enabled: true, // You can make this configurable
	}
}

// RecordMessageProcessed sends metrics about processed messages
func (d *DynatraceClient) RecordMessageProcessed(topic string, processingTimeMs float64, success bool) {
	if !d.enabled {
		return
	}

	// Create dimensions for the metric
	dims := dimensions.NewNormalizedDimensionList(
		dimensions.NewDimension("topic", topic),
		dimensions.NewDimension("service", "data-enricher"),
		dimensions.NewDimension("success", fmt.Sprintf("%t", success)),
	)

	// Enrich with OneAgent data if available
	//enrichedDims := oneagentenrichment.NewEnrichmentInfo().AddDimensionsToNormalizedDimensionList(dims)
	enrichedDims := oneagentenrichment.GetOneAgentMetadata()

	// Create and send counter metric for message count
	countMetric, err := metric.NewMetric(
		"dataenricher.messages.processed.count",
		metric.WithDimensions(enrichedDims),
		metric.WithDimensions(dims),
		metric.WithTimestamp(time.Now()),
	)
	if err != nil {
		d.logger.Warn().Err(err).Msg("Failed to create message count metric")
		return
	}

	_, err = countMetric.Serialize()
	if err != nil {
		d.logger.Warn().Err(err).Msg("Failed to serialize message count metric")
		return
	}

	// Create and send gauge metric for processing time
	if processingTimeMs > 0 {
		timeMetric, err := metric.NewMetric(
			"dataenricher.processing.duration.ms",
			metric.WithDimensions(enrichedDims),
			metric.WithTimestamp(time.Now()),
			metric.WithFloatGaugeValue(processingTimeMs),
		)

		_, err = timeMetric.Serialize()
		if err != nil {
			d.logger.Warn().Err(err).Msg("Failed to serialize processing time metric")
		}

	}

	d.logger.Debug().
		Str("topic", topic).
		Float64("processing_time_ms", processingTimeMs).
		Bool("success", success).
		Msg("Sent metrics to Dynatrace")
}

// RecordError sends error metrics
func (d *DynatraceClient) RecordError(errorType, topic string) {
	if !d.enabled {
		return
	}

	dims := dimensions.NewNormalizedDimensionList(
		dimensions.NewDimension("error_type", errorType),
		dimensions.NewDimension("topic", topic),
		dimensions.NewDimension("service", "data-enricher"),
	)

	//enrichedDims := oneagentenrichment.NewEnrichmentInfo().AddDimensionsToNormalizedDimensionList(dims)
	enrichedDims := oneagentenrichment.GetOneAgentMetadata()

	errorMetric, err := metric.NewMetric(
		"dataenricher.errors.count",
		metric.WithDimensions(enrichedDims),
		metric.WithDimensions(dims),
		metric.WithTimestamp(time.Now()),
	)

	if err != nil {
		d.logger.Warn().Err(err).Msg("Failed to create error metric")
		return
	}
	_, err = errorMetric.Serialize()
	if err != nil {
		d.logger.Warn().Err(err).Msg("Failed to serialize error metric")
		return
	}

	d.logger.Debug().
		Str("error_type", errorType).
		Str("topic", topic).
		Msg("Sent error metric to Dynatrace")
}

// RecordConnectionStatus sends connection status metrics
func (d *DynatraceClient) RecordConnectionStatus(service string, connected bool) {
	if !d.enabled {
		return
	}

	dims := dimensions.NewNormalizedDimensionList(
		dimensions.NewDimension("service_type", service),
		dimensions.NewDimension("status", fmt.Sprintf("%t", connected)),
		dimensions.NewDimension("application", "data-enricher"),
	)

	//enrichedDims := oneagentenrichment.NewEnrichmentInfo().AddDimensionsToNormalizedDimensionList(dims)
	enrichedDims := oneagentenrichment.GetOneAgentMetadata()

	statusValue := float64(0)
	if connected {
		statusValue = 1
	}

	statusMetric, err := metric.NewMetric(
		"dataenricher.connection.status",
		metric.WithDimensions(enrichedDims),
		metric.WithDimensions(dims),
		metric.WithTimestamp(time.Now()),
		metric.WithFloatGaugeValue(statusValue),
	)
	if err != nil {
		d.logger.Warn().Err(err).Msg("Failed to create connection status metric")
		return
	}

	_, err = statusMetric.Serialize()
	if err != nil {
		d.logger.Warn().Err(err).Msg("Failed to serialize connection status metric")
		return
	}

	d.logger.Debug().
		Str("service", service).
		Bool("connected", connected).
		Msg("Sent connection status metric to Dynatrace")
}

// RecordCacheOperation sends Redis cache operation metrics
func (d *DynatraceClient) RecordCacheOperation(operation string, success bool, durationMs float64) {
	if !d.enabled {
		return
	}

	dims := dimensions.NewNormalizedDimensionList(
		dimensions.NewDimension("operation", operation),
		dimensions.NewDimension("success", fmt.Sprintf("%t", success)),
		dimensions.NewDimension("service", "data-enricher"),
		dimensions.NewDimension("component", "redis"),
	)

	//enrichedDims := oneagentenrichment.NewEnrichmentInfo().AddDimensionsToNormalizedDimensionList(dims)
	enrichedDims := oneagentenrichment.GetOneAgentMetadata()

	// Send operation count
	countMetric, err := metric.NewMetric(
		"dataenricher.cache.operations.count",
		metric.WithDimensions(enrichedDims),
		metric.WithDimensions(dims),
		metric.WithTimestamp(time.Now()),
	)
	if err != nil {
		d.logger.Warn().Err(err).Msg("Failed to create cache operation count metric")
		return
	}
	_, err = countMetric.Serialize()
	if err != nil {
		d.logger.Warn().Err(err).Msg("Failed to serialize cache operation count metric")
	}

	// Send operation duration if provided
	if durationMs > 0 {
		durationMetric, err := metric.NewMetric(
			"dataenricher.cache.operation.duration.ms",
			metric.WithDimensions(enrichedDims),
			metric.WithTimestamp(time.Now()),
			metric.WithDimensions(dims),
			metric.WithFloatGaugeValue(durationMs),
		)
		if err != nil {
			d.logger.Warn().Err(err).Msg("Failed to create cache operation duration metric")
			return
		}
		_, err = durationMetric.Serialize()
		if err != nil {
			d.logger.Warn().Err(err).Msg("Failed to serialize cache operation duration metric")
		}
	}
}

// Disable disables metric collection
func (d *DynatraceClient) Disable() {
	d.enabled = false
	d.logger.Info().Msg("Dynatrace metrics disabled")
}

// Enable enables metric collection
func (d *DynatraceClient) Enable() {
	d.enabled = true
	d.logger.Info().Msg("Dynatrace metrics enabled")
}
