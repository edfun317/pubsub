package queuesdk

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
	"github.com/ThreeDotsLabs/watermill/message"
)

// GCPPubSubFactory GCP Pub/Sub factory implementation
type GCPPubSubFactory struct{}

// CreatePublisher creates GCP Pub/Sub Publisher
func (f *GCPPubSubFactory) CreatePublisher(config Config, logger watermill.LoggerAdapter) (message.Publisher, error) {

	gcpConfig, ok := config.(GCPConfig)
	if !ok {
		return nil, fmt.Errorf("invalid GCP config type")
	}

	if err := gcpConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid GCP config: %w", err)
	}

	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	publisher, err := googlecloud.NewPublisher(
		googlecloud.PublisherConfig{
			ProjectID: gcpConfig.ProjectID,
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP publisher: %w", err)
	}

	return publisher, nil
}

// CreateSubscriber creates GCP Pub/Sub Subscriber
func (f *GCPPubSubFactory) CreateSubscriber(config Config, logger watermill.LoggerAdapter) (message.Subscriber, error) {

	gcpConfig, ok := config.(GCPConfig)
	if !ok {
		return nil, fmt.Errorf("invalid GCP config type")
	}

	if err := gcpConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid GCP config: %w", err)
	}

	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	subscriber, err := googlecloud.NewSubscriber(
		googlecloud.SubscriberConfig{
			ProjectID: gcpConfig.ProjectID,
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP subscriber: %w", err)
	}

	return subscriber, nil
}
