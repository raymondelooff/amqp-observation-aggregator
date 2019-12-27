package aggregator

import (
	"fmt"
	"log"

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
	deliveries chan amqp.Delivery
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
	name := s.queue.Name

	_, err := s.channel.QueueDelete(name, true, false, false)

	if err != nil {
		log.Printf("Subscriber: %s", err)

		return fmt.Errorf("Subscriber: failed to delete Queue")
	}

	return nil
}

// Subscribe to the topics defined in the AMQPConfig
func (s *Subscriber) Subscribe() (chan amqp.Delivery, error) {
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

			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// Shutdown the Subscriber
func (s *Subscriber) Shutdown() error {
	log.Printf("Subscriber: shutting down")

	if s.connection == nil {
		log.Printf("Subscriber: shutdown OK")

		return nil
	}

	err := s.deleteQueue()
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
		deliveries: make(chan amqp.Delivery),
	}
}
