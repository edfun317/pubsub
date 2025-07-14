package queuesdk

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// MessageQueueFactory factory pattern interface
type MessageQueueFactory interface {
	CreatePublisher(config Config, logger watermill.LoggerAdapter) (message.Publisher, error)
	CreateSubscriber(config Config, logger watermill.LoggerAdapter) (message.Subscriber, error)
}

// getFactory returns the corresponding factory based on configuration type
func getFactory(configType string) MessageQueueFactory {
	switch configType {
	case "gcp":
		return &GCPPubSubFactory{}
	case "kafka":
		return &KafkaFactory{}
	default:
		return nil
	}
}
