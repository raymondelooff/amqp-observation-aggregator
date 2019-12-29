package aggregator

import (
	"time"
)

// ObservationUpdate represents a single observation update message
type ObservationUpdate struct {
	ObservationType string
	State           float64
	MeasuredAt      time.Time
}
