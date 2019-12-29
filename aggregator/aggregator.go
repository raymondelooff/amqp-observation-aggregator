package aggregator

import (
	"database/sql"
	"log"
	"sync"
)

// Config is the main configuration
type Config struct {
	AMQP   AMQPConfig   `yaml:"amqp"`
	MySQL  MySQLConfig  `yaml:"mysql"`
	Writer WriterConfig `yaml:"writer"`
	Topics []string     `yaml:"topics"`
}

// Aggregator uses a Subscriber and Writer to aggregate observations
type Aggregator struct {
	config     Config
	subscriber *Subscriber
	writer     *Writer
}

// Run the Aggregator
func (a *Aggregator) Run(wg *sync.WaitGroup) {
	observationUpdates, err := a.subscriber.Subscribe()
	if err != nil {
		log.Fatalf("aggregator: %s", err)
	}

	defer a.subscriber.Shutdown()

	go func() {
		for observationUpdate := range observationUpdates {
			a.handleObservationUpdate(&observationUpdate, wg)
		}
	}()

	wg.Wait()
}

// Handles the given message
func (a *Aggregator) handleObservationUpdate(observationUpdate *ObservationUpdate, wg *sync.WaitGroup) {
	wg.Add(1)

	log.Println(observationUpdate)

	wg.Done()
}

// NewAggregator creates a new Aggregator
func NewAggregator(config Config, db *sql.DB) *Aggregator {
	return &Aggregator{
		config:     config,
		subscriber: NewSubscriber(config.AMQP, config.Topics),
		writer:     NewWriter(config.Writer, db),
	}
}
