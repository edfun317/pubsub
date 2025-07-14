package queuesdk

import (
	"fmt"
)

// Config unified configuration interface
type Config interface {
	GetType() string // "gcp", "kafka"
	Validate() error
}

// MessageQueueConfig for configuration-driven approach
type MessageQueueConfig struct {
	Type   string      `yaml:"type"`   // "gcp", "kafka"
	Config interface{} `yaml:"config"` // specific configuration
}

// ServiceConfig service configuration
type ServiceConfig struct {
	MessageQueue MessageQueueConfig `yaml:"message_queue"`
}

// GCPConfig GCP Pub/Sub specific configuration
type GCPConfig struct {
	ProjectID string `json:"project_id" yaml:"project_id"`
}

func (c GCPConfig) GetType() string {
	return "gcp"
}

func (c GCPConfig) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("project_id is required for GCP provider")
	}
	return nil
}

// KafkaConfig Kafka specific configuration
type KafkaConfig struct {
	Brokers       []string `json:"brokers" yaml:"brokers"`
	ConsumerGroup string   `json:"consumer_group" yaml:"consumer_group"`
}

func (c KafkaConfig) GetType() string {
	return "kafka"
}

func (c KafkaConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers are required for Kafka provider")
	}
	if c.ConsumerGroup == "" {
		return fmt.Errorf("consumer_group is required for Kafka provider")
	}
	return nil
}
