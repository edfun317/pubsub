package queuesdk

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// MessageQueueSDK main SDK structure - Builder Pattern
type MessageQueueSDK struct {
	factory MessageQueueFactory
	config  Config
	logger  watermill.LoggerAdapter
}

// NewMessageQueueSDK creates a new SDK instance
func NewMessageQueueSDK() *MessageQueueSDK {
	return &MessageQueueSDK{
		logger: watermill.NewStdLogger(false, false),
	}
}

// UseGCP uses GCP Pub/Sub
func (sdk *MessageQueueSDK) UseGCP(config GCPConfig) *MessageQueueSDK {
	sdk.factory = &GCPPubSubFactory{}
	sdk.config = config
	return sdk
}

// UseKafka uses Kafka
func (sdk *MessageQueueSDK) UseKafka(config KafkaConfig) *MessageQueueSDK {
	sdk.factory = &KafkaFactory{}
	sdk.config = config
	return sdk
}

// WithLogger sets logger
func (sdk *MessageQueueSDK) WithLogger(logger watermill.LoggerAdapter) *MessageQueueSDK {
	sdk.logger = logger
	return sdk
}

// Build builds Publisher and Subscriber
func (sdk *MessageQueueSDK) Build() (message.Publisher, message.Subscriber, error) {
	if sdk.factory == nil {
		return nil, nil, fmt.Errorf("no message queue provider selected")
	}

	if sdk.config == nil {
		return nil, nil, fmt.Errorf("no configuration provided")
	}

	pub, err := sdk.factory.CreatePublisher(sdk.config, sdk.logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	sub, err := sdk.factory.CreateSubscriber(sdk.config, sdk.logger)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create subscriber: %w", err)
	}

	return pub, sub, nil
}

// BuildPublisher builds Publisher only
func (sdk *MessageQueueSDK) BuildPublisher() (message.Publisher, error) {
	if sdk.factory == nil {
		return nil, fmt.Errorf("no message queue provider selected")
	}

	if sdk.config == nil {
		return nil, fmt.Errorf("no configuration provided")
	}

	pub, err := sdk.factory.CreatePublisher(sdk.config, sdk.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	return pub, nil
}

// BuildSubscriber builds Subscriber only
func (sdk *MessageQueueSDK) BuildSubscriber() (message.Subscriber, error) {
	if sdk.factory == nil {
		return nil, fmt.Errorf("no message queue provider selected")
	}

	if sdk.config == nil {
		return nil, fmt.Errorf("no configuration provided")
	}

	sub, err := sdk.factory.CreateSubscriber(sdk.config, sdk.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscriber: %w", err)
	}

	return sub, nil
}
