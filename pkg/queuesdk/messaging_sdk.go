package queuesdk

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// MessagingSDK main SDK interface
type MessagingSDK interface {
	Publisher() message.Publisher
	Subscriber() message.Subscriber
	Router() *message.Router
	//	Health() HealthChecker
	Close() error
}

// messagingSDK implementation
type messagingSDK struct {
	publisher  message.Publisher
	subscriber message.Subscriber
	router     *message.Router
	//health     HealthChecker
	logger watermill.LoggerAdapter
}

// NewMessagingSDK initializes SDK
func NewMessagingSDK(config Config, logger watermill.LoggerAdapter) (MessagingSDK, error) {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	factory := getFactory(config.GetType())
	if factory == nil {
		return nil, fmt.Errorf("unsupported message queue type: %s", config.GetType())
	}

	pub, err := factory.CreatePublisher(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	sub, err := factory.CreateSubscriber(config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscriber: %w", err)
	}

	// Create Router
	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	// Create health checker
	//healthChecker := newHealthChecker(pub, sub, logger)

	return &messagingSDK{
		publisher:  pub,
		subscriber: sub,
		router:     router,
		//health:     healthChecker,
		logger: logger,
	}, nil
}

// Publisher returns Publisher
func (sdk *messagingSDK) Publisher() message.Publisher {
	return sdk.publisher
}

// Subscriber returns Subscriber
func (sdk *messagingSDK) Subscriber() message.Subscriber {
	return sdk.subscriber
}

// Router returns Router
func (sdk *messagingSDK) Router() *message.Router {
	return sdk.router
}

// Health returns health checker
/* func (sdk *messagingSDK) Health() HealthChecker {
	return sdk.health
} */

// Close closes all resources
func (sdk *messagingSDK) Close() error {
	var errs []error

	// Close Router
	if err := sdk.router.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close router: %w", err))
	}

	// Close Publisher
	if err := sdk.publisher.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close publisher: %w", err))
	}

	// Close Subscriber
	if err := sdk.subscriber.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close subscriber: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while closing SDK: %v", errs)
	}

	return nil
}

// NewMessagingSDKFromBuilder creates SDK from Builder
func NewMessagingSDKFromBuilder(builder *MessageQueueSDK) (MessagingSDK, error) {
	if builder.config == nil {
		return nil, fmt.Errorf("no configuration provided")
	}

	return NewMessagingSDK(builder.config, builder.logger)
}

// StartRouter starts Router
func (sdk *messagingSDK) StartRouter(ctx context.Context) error {
	go func() {
		if err := sdk.router.Run(ctx); err != nil {
			sdk.logger.Error("Router error", err, nil)
		}
	}()

	// Wait for Router to start
	<-sdk.router.Running()
	return nil
}
