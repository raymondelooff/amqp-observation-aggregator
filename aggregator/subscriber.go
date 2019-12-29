package aggregator

import (
	"fmt"
	"log"
	"time"

	"github.com/segmentio/encoding/json"

	"github.com/avast/retry-go"
	"github.com/streadway/amqp"
)

// AMQPConfig represents the config of the Subscriber
type AMQPConfig struct {
	Tag      string `yaml:"tag"`
	Exchange string `yaml:"exchange"`
	DSN      string `yaml:"dsn"`
	TLS      bool   `yaml:"tls"`
}

// Subscriber represents an AMQP subscriber
type Subscriber struct {
	config     AMQPConfig
	topics     []string
	tag        string
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      *amqp.Queue
}

// Connect with the configured AMQP broker
func (s *Subscriber) dial() error {
	var err error

	if s.config.TLS == true {
		s.connection, err = amqp.DialTLS(s.config.DSN, nil)
	} else {
		s.connection, err = amqp.Dial(s.config.DSN)
	}
	if err != nil {
		return fmt.Errorf("Subscriber: %v", err)
	}

	log.Printf("Subscriber: connection established")

	return nil
}

// Get a Channel for the deliveries
func (s *Subscriber) getChannel() error {
	var err error

	s.channel, err = s.connection.Channel()
	if err != nil {
		log.Printf("Subscriber: %s", err)

		return fmt.Errorf("Subscriber: failed to get Channel")
	}

	log.Printf("Subscriber: got Channel")

	return nil
}

// Declare a non-durable Queue for the deliveries
func (s *Subscriber) declareQueue() (*amqp.Queue, error) {
	var queue amqp.Queue
	var err error

	queueName := fmt.Sprintf("amqp-observation-aggregator-%s", s.tag)
	log.Printf("Subscriber: declaring Queue %v", queueName)

	queue, err = s.channel.QueueDeclare(
		queueName,
		false, // durable
		true,  // autoDelete
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		log.Printf("Subscriber: %s", err)

		return nil, fmt.Errorf("Subscriber: failed to declare Queue")
	}

	log.Printf("Subscriber: declared Queue")

	return &queue, nil
}

// Bind the Queue to the configured topics
func (s *Subscriber) bindQueue() error {
	var err error

	if s.queue == nil {
		return fmt.Errorf("Subscriber: Queue not declared")
	}

	for _, topic := range s.topics {
		log.Printf("Subscriber: binding topic to Exchange (key: %q)", topic)

		err = s.channel.QueueBind(
			s.queue.Name,      // name
			topic,             // key
			s.config.Exchange, // exchange
			false,             // noWait
			nil,               // arguments
		)
		if err != nil {
			log.Printf("Subscriber: %s", err)

			return fmt.Errorf("Subscriber: failed to bind Queue")
		}
	}

	return nil
}

// Delete the declared Queue if there a no more consumers
func (s *Subscriber) deleteQueue() error {
	_, err := s.channel.QueueDelete(s.queue.Name, true, false, false)

	if err != nil {
		log.Printf("Subscriber: %s", err)

		return fmt.Errorf("Subscriber: failed to delete Queue")
	}

	return nil
}

// Consume the deliveries
func (s *Subscriber) consume() (<-chan amqp.Delivery, error) {
	deliveries, err := s.channel.Consume(
		s.queue.Name, // queue
		s.tag,        // consumer,
		false,        // autoAck
		false,        // exclusive
		false,        // noLocal
		false,        // noWait
		nil,          // arguments
	)

	if err != nil {
		log.Printf("Subscriber: %s", err)

		return nil, fmt.Errorf("Subscriber: failed to consume")
	}

	return deliveries, nil
}

// Cancels the current consumer
func (s *Subscriber) cancelConsumer() error {
	err := s.channel.Cancel(s.tag, false)

	if err != nil {
		log.Printf("Subscriber: %s", err)

		return fmt.Errorf("Subscriber: failed to cancel consumer")
	}

	return nil
}

// Parses the given ISO8601 time string into a Time
func (s *Subscriber) parseTime(data string) (*time.Time, error) {
	measuredAt, err := time.Parse(time.RFC3339, data)
	if err != nil {
		return nil, err
	}

	return &measuredAt, nil
}

// Handles the given AMQP Delivery
func (s *Subscriber) handleDelivery(delivery *amqp.Delivery) (*ObservationUpdate, error) {
	var data map[string]interface{}

	if err := json.Unmarshal(delivery.Body, &data); err != nil {
		return nil, fmt.Errorf("Subscriber: error unmarshalling delivery body")
	}

	value, ok := data["value"]
	if !ok {
		return nil, fmt.Errorf("Subscriber: key 'value' not found in map")
	}
	if value == nil {
		return nil, fmt.Errorf("Subscriber: 'value' is nil")
	}

	timestamp, ok := data["timestamp"]
	if !ok {
		return nil, fmt.Errorf("Subscriber: key 'timestamp' not found in map")
	}
	if timestamp == nil {
		return nil, fmt.Errorf("Subscriber: 'timestamp' is nil")
	}

	measuredAt, err := s.parseTime(timestamp.(string))
	if err != nil {
		return nil, fmt.Errorf("Subscriber: could not parse 'timestamp' time")
	}

	return &ObservationUpdate{
		State:      value.(float64),
		MeasuredAt: *measuredAt,
	}, nil
}

// Subscribe to the topics defined in the AMQPConfig
func (s *Subscriber) Subscribe() (<-chan ObservationUpdate, error) {
	var deliveries <-chan amqp.Delivery

	err := s.dial()
	if err != nil {
		return nil, err
	}

	err = retry.Do(
		func() error {
			err = s.getChannel()
			if err != nil {
				return err
			}

			s.queue, err = s.declareQueue()
			if err != nil {
				return err
			}

			err = s.bindQueue()
			if err != nil {
				return err
			}

			deliveries, err = s.consume()
			if err != nil {
				return err
			}

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	observationUpdates := make(chan ObservationUpdate)

	go func() {
		for delivery := range deliveries {
			observationUpdate, err := s.handleDelivery(&delivery)
			if err != nil {
				continue
			}

			observationUpdates <- *observationUpdate
		}
	}()

	return observationUpdates, nil
}

// Shutdown the Subscriber
func (s *Subscriber) Shutdown() error {
	log.Printf("Subscriber: shutting down")

	if s.connection == nil {
		log.Printf("Subscriber: shutdown OK")

		return nil
	}

	var err error

	err = s.cancelConsumer()
	if err != nil {
		return err
	}

	err = s.deleteQueue()
	if err != nil {
		return err
	}

	if err := s.connection.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	log.Printf("Subscriber: shutdown OK")

	return nil
}

// NewSubscriber creates a new Subscriber
func NewSubscriber(config AMQPConfig, topics []string) *Subscriber {
	return &Subscriber{
		config:     config,
		topics:     topics,
		tag:        config.Tag,
		connection: nil,
		channel:    nil,
	}
}
