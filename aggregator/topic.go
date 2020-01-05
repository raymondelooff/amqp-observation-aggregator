package aggregator

import (
	"fmt"
	"regexp"
)

// Topic represents an AMQP topic
type Topic struct {
	stationRegex *regexp.Regexp

	Value string
}

// GetStationID returns the StationID from the Topic value
func (t *Topic) GetStationID() (string, error) {
	matches := t.stationRegex.FindStringSubmatch(t.Value)

	if matches == nil {
		return "", fmt.Errorf("Topic: '%s' does not match topic regex", t.Value)
	}

	if len(matches) < 2 {
		return "", fmt.Errorf("Topic: StationID not found in topic")
	}

	return matches[1], nil
}

// NewTopic constructs a new Topic
func NewTopic(value string) *Topic {
	return &Topic{
		stationRegex: regexp.MustCompile(`^(\w+)\..*$`),
		Value:        value,
	}
}
