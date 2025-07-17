package queuesdk

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
)

// KafkaFactory Kafka factory implementation
type KafkaFactory struct{}

// CreatePublisher creates Kafka Publisher
func (f *KafkaFactory) CreatePublisher(config Config, logger watermill.LoggerAdapter) (message.Publisher, error) {
	kafkaConfig, ok := config.(KafkaConfig)
	if !ok {
		return nil, fmt.Errorf("invalid Kafka config type")
	}

	if err := kafkaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Kafka config: %w", err)
	}

	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	publisherConfig := kafka.PublisherConfig{
		Brokers:   kafkaConfig.Brokers,
		Marshaler: kafka.DefaultMarshaler{},
	}

	publisher, err := kafka.NewPublisher(publisherConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka publisher: %w", err)
	}

	return publisher, nil
}

// CreateSubscriber creates Kafka Subscriber
func (f *KafkaFactory) CreateSubscriber(config Config, logger watermill.LoggerAdapter) (message.Subscriber, error) {
	kafkaConfig, ok := config.(KafkaConfig)
	if !ok {
		return nil, fmt.Errorf("invalid Kafka config type")
	}

	if err := kafkaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Kafka config: %w", err)
	}

	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	subscriberConfig := kafka.SubscriberConfig{
		Brokers:               kafkaConfig.Brokers,
		Unmarshaler:           kafka.DefaultMarshaler{},
		ConsumerGroup:         kafkaConfig.ConsumerGroup,
		OverwriteSaramaConfig: nil,
	}

	subscriber, err := kafka.NewSubscriber(subscriberConfig, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka subscriber: %w", err)
	}

	return subscriber, nil
}
