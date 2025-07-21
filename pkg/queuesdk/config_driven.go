package queuesdk

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// InitializeMessageQueue configuration-driven initialization method
func InitializeMessageQueue(config ServiceConfig, logger watermill.LoggerAdapter) (message.Publisher, message.Subscriber, error) {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	switch config.MessageQueue.Type {
	case "gcp":
		return initGCPPubSub(config.MessageQueue.Config, logger)
	case "kafka":
		return initKafka(config.MessageQueue.Config, logger)
	default:
		return nil, nil, fmt.Errorf("unsupported message queue type: %s", config.MessageQueue.Type)
	}
}

// initGCPPubSub initializes GCP Pub/Sub
func initGCPPubSub(configData interface{}, logger watermill.LoggerAdapter) (message.Publisher, message.Subscriber, error) {
	// This needs to convert interface{} to GCPConfig
	// In actual usage, mapstructure or other methods may be needed to handle YAML config
	gcpConfig, ok := configData.(GCPConfig)
	if !ok {
		return nil, nil, fmt.Errorf("invalid GCP configuration format")
	}

	factory := &GCPPubSubFactory{}
	publisher, err := factory.CreatePublisher(gcpConfig, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create GCP publisher: %w", err)
	}

	subscriber, err := factory.CreateSubscriber(gcpConfig, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create GCP subscriber: %w", err)
	}

	return publisher, subscriber, nil
}

// initKafka initializes Kafka
func initKafka(configData interface{}, logger watermill.LoggerAdapter) (message.Publisher, message.Subscriber, error) {
	// This needs to convert interface{} to KafkaConfig
	kafkaConfig, ok := configData.(KafkaConfig)
	if !ok {
		return nil, nil, fmt.Errorf("invalid Kafka configuration format")
	}

	factory := &KafkaFactory{}
	publisher, err := factory.CreatePublisher(kafkaConfig, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Kafka publisher: %w", err)
	}

	subscriber, err := factory.CreateSubscriber(kafkaConfig, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Kafka subscriber: %w", err)
	}

	return publisher, subscriber, nil
}

// CreateFromConfig creates directly using Config interface
func CreateFromConfig(config Config, logger watermill.LoggerAdapter) (message.Publisher, message.Subscriber, error) {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	factory := getFactory(config.GetType())
	if factory == nil {
		return nil, nil, fmt.Errorf("unsupported message queue type: %s", config.GetType())
	}

	publisher, err := factory.CreatePublisher(config, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	subscriber, err := factory.CreateSubscriber(config, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create subscriber: %w", err)
	}

	return publisher, subscriber, nil
}
