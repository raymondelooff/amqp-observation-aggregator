package aggregator

import (
	"database/sql"
	"sync"

	"go.uber.org/zap"
)

// Config is the main configuration
type Config struct {
	Env       string       `yaml:"env"`
	SentryDsn string       `yaml:"sentry_dsn"`
	AMQP      AMQPConfig   `yaml:"amqp"`
	MySQL     MySQLConfig  `yaml:"mysql"`
	Writer    WriterConfig `yaml:"writer"`
	Topics    []string     `yaml:"topics"`
}

// Aggregator uses a Subscriber and Writer to aggregate observations
type Aggregator struct {
	config     Config
	subscriber *Subscriber
	writer     *Writer
	logger     *zap.SugaredLogger
}

// Run the Aggregator
func (a *Aggregator) Run(wg *sync.WaitGroup) {
	observationUpdates, err := a.subscriber.Subscribe()
	if err != nil {
		a.logger.Fatalf("aggregator: %s", err)
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

	a.writer.Write(observationUpdate)

	wg.Done()
}

// NewAggregator creates a new Aggregator
func NewAggregator(config Config, db *sql.DB, logger *zap.SugaredLogger) *Aggregator {
	return &Aggregator{
		config:     config,
		subscriber: NewSubscriber(config.AMQP, config.Topics, logger),
		writer:     NewWriter(config.Writer, db, logger),
		logger:     logger,
	}
}
