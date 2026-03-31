// Package config holds csar-audit YAML configuration.
package config

import (
	"fmt"
	"reflect"
	"time"

	"github.com/ledatu/csar-core/configutil"
	"github.com/ledatu/csar-core/tlsx"
	"gopkg.in/yaml.v3"
)

// Config is the top-level service configuration.
type Config struct {
	Service  ServiceConfig   `yaml:"service"`
	TLS      TLSSection      `yaml:"tls"`
	GRPC     GRPCConfig      `yaml:"grpc"`
	Database DatabaseConfig  `yaml:"database"`
	RabbitMQ RabbitMQConfig  `yaml:"rabbitmq"`
	Ingest   IngestConfig    `yaml:"ingest"`
	Consumer ConsumerConfig  `yaml:"consumer"`
	HTTP     HTTPExtraConfig `yaml:"http"`
	Tracing  TracingConfig   `yaml:"tracing"`
}

// ServiceConfig describes HTTP and health listen ports.
type ServiceConfig struct {
	Name       string `yaml:"name"`
	Port       int    `yaml:"port"`
	HealthPort int    `yaml:"health_port"`
}

// TLSSection mirrors tlsx.ServerConfig with YAML tags.
type TLSSection struct {
	CertFile     string `yaml:"cert_file"`
	KeyFile      string `yaml:"key_file"`
	ClientCAFile string `yaml:"client_ca_file"`
	MinVersion   string `yaml:"min_version"`
}

// GRPCConfig holds the gRPC listen port and TLS for the ingest RPC surface.
type GRPCConfig struct {
	Port       int        `yaml:"port"`
	TLS        TLSSection `yaml:"tls"`
	Reflection bool       `yaml:"reflection"`
}

// DatabaseConfig holds Postgres DSN.
type DatabaseConfig struct {
	DSN string `yaml:"dsn"`
}

// RabbitMQConfig holds broker URL and reconnect tuning.
type RabbitMQConfig struct {
	URL            string              `yaml:"url"`
	ReconnectDelay configutil.Duration `yaml:"reconnect_delay"`
}

// IngestConfig configures the in-memory buffer and publisher workers.
type IngestConfig struct {
	BufferSize       int `yaml:"buffer_size"`
	PublisherWorkers int `yaml:"publisher_workers"`
}

// ConsumerConfig configures the batch consumer reading from RabbitMQ.
type ConsumerConfig struct {
	Queue           ConsumerQueueConfig `yaml:"queue"`
	DLQ             ConsumerDLQConfig   `yaml:"dlq"`
	BatchSize       int                 `yaml:"batch_size"`
	FlushInterval   configutil.Duration `yaml:"flush_interval"`
	MaxRedeliveries int                 `yaml:"max_redeliveries"`
}

// ConsumerQueueConfig is the primary audit queue.
type ConsumerQueueConfig struct {
	Name     string `yaml:"name"`
	Durable  bool   `yaml:"durable"`
	Prefetch int    `yaml:"prefetch"`
}

// ConsumerDLQConfig is the dead-letter queue for poison messages.
type ConsumerDLQConfig struct {
	Name    string `yaml:"name"`
	Durable bool   `yaml:"durable"`
}

// HTTPExtraConfig augments HTTP server behavior (mTLS gateway verification).
type HTTPExtraConfig struct {
	AllowedClientCN string `yaml:"allowed_client_cn"`
}

// TracingConfig configures OTLP export.
type TracingConfig struct {
	Endpoint    string  `yaml:"endpoint"`
	SampleRatio float64 `yaml:"sample_ratio"`
}

// LoadFromBytes parses YAML and expands ${VAR} placeholders in string fields.
func LoadFromBytes(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	configutil.ExpandEnvInStruct(reflect.ValueOf(&cfg).Elem())
	defaults(&cfg)
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func defaults(cfg *Config) {
	if cfg.Service.Name == "" {
		cfg.Service.Name = "csar-audit"
	}
	if cfg.Service.Port == 0 {
		cfg.Service.Port = 8083
	}
	if cfg.Service.HealthPort == 0 {
		cfg.Service.HealthPort = 9083
	}
	if cfg.GRPC.Port == 0 {
		cfg.GRPC.Port = 9084
	}
	if cfg.Ingest.BufferSize == 0 {
		cfg.Ingest.BufferSize = 10_000
	}
	if cfg.Ingest.PublisherWorkers == 0 {
		cfg.Ingest.PublisherWorkers = 4
	}
	if cfg.Consumer.Queue.Name == "" {
		cfg.Consumer.Queue.Name = "audit.events"
	}
	if cfg.Consumer.Queue.Prefetch == 0 {
		cfg.Consumer.Queue.Prefetch = 200
	}
	if cfg.Consumer.DLQ.Name == "" {
		cfg.Consumer.DLQ.Name = "audit.events.dlq"
	}
	if cfg.Consumer.BatchSize == 0 {
		cfg.Consumer.BatchSize = 200
	}
	if cfg.Consumer.FlushInterval.Std() == 0 {
		cfg.Consumer.FlushInterval.Duration = 2 * time.Second
	}
	if cfg.Consumer.MaxRedeliveries == 0 {
		cfg.Consumer.MaxRedeliveries = 3
	}
}

func (c *Config) validate() error {
	if c.Database.DSN == "" {
		return fmt.Errorf("database.dsn is required")
	}
	if c.RabbitMQ.URL == "" {
		return fmt.Errorf("rabbitmq.url is required")
	}
	if c.Ingest.BufferSize < 1 {
		return fmt.Errorf("ingest.buffer_size must be >= 1")
	}
	if c.Ingest.PublisherWorkers < 1 {
		return fmt.Errorf("ingest.publisher_workers must be >= 1")
	}
	if c.Consumer.BatchSize < 1 {
		return fmt.Errorf("consumer.batch_size must be >= 1")
	}
	return nil
}

// HTTPAddr returns the HTTP listen address (e.g. ":8083").
func (c *Config) HTTPAddr() string {
	return fmt.Sprintf(":%d", c.Service.Port)
}

// HealthAddr returns the health sidecar listen address.
func (c *Config) HealthAddr() string {
	return fmt.Sprintf(":%d", c.Service.HealthPort)
}

// GRPCAddr returns the gRPC listen address.
func (c *Config) GRPCAddr() string {
	return fmt.Sprintf(":%d", c.GRPC.Port)
}

// ToTLSx converts the HTTP TLS section.
func (t TLSSection) ToTLSx() tlsx.ServerConfig {
	return tlsx.ServerConfig{
		CertFile:     t.CertFile,
		KeyFile:      t.KeyFile,
		ClientCAFile: t.ClientCAFile,
		MinVersion:   t.MinVersion,
	}
}
