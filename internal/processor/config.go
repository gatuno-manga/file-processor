package processor

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/h2non/bimg"
)

type Config struct {
	AppEnv           string
	Port             string
	HealthPort       string
	PoolSize         int
	KafkaBrokers     []string
	KafkaGroupID     string
	KafkaInputTopic  string
	KafkaOutputTopic string
	KafkaDocInput    string
	KafkaDocOutput   string
	// KafkaStartFromBeginning sets StartOffset to FirstOffset when true.
	// Should be false in production to avoid reprocessing on GroupID changes.
	KafkaStartFromBeginning bool
	// KafkaNumPartitions controls the number of partitions for auto-created topics (dev only).
	KafkaNumPartitions int
	// KafkaReplicationFactor controls the replication factor for auto-created topics (dev only).
	KafkaReplicationFactor int
	StorageEndpoint        string
	StorageAccessKey       string
	StorageSecretKey       string
	StorageSSL             bool
	VipsMaxCache           int
	VipsMaxCacheMem        int
	WebPQuality            int
	// MaxImageTasks limits concurrent image processing goroutines in the Kafka consumer.
	MaxImageTasks int
	// MaxDocumentTasks limits concurrent document processing goroutines in the Kafka consumer.
	MaxDocumentTasks int
	// ProcessTimeout bounds how long a single Kafka message handler may run.
	// Must exceed the p99.9 of file_processor_duration_seconds plus S3 round-trips.
	ProcessTimeout time.Duration
	// KafkaDLQSuffix is appended to the input topic name to derive the dead-letter topic.
	KafkaDLQSuffix string
	// MaxDeliveryTries caps in-message retries before a message is routed to the DLQ.
	MaxDeliveryTries int
	// RetryBackoff is the initial delay between retries; it doubles after each attempt.
	RetryBackoff time.Duration
}

func LoadConfig() *Config {
	poolSize := getEnvInt("WORKER_POOL_SIZE", 0)

	// Default image tasks: 2x pool size, fallback to 16 if pool size is 0.
	maxImageTasks := getEnvInt("MAX_IMAGE_TASKS", 0)
	if maxImageTasks <= 0 {
		if poolSize > 0 {
			maxImageTasks = poolSize * 2
		} else {
			maxImageTasks = 16
		}
	}

	// Default document tasks: lighter workload, 4 concurrent by default.
	maxDocumentTasks := getEnvInt("MAX_DOCUMENT_TASKS", 4)
	if maxDocumentTasks <= 0 {
		maxDocumentTasks = 4
	}

	return &Config{
		AppEnv:                  getEnv("APP_ENV", "development"),
		Port:                    getEnv("GRPC_PORT", "50051"),
		HealthPort:              getEnv("HEALTH_PORT", "8081"),
		PoolSize:                poolSize,
		KafkaBrokers:            strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaGroupID:            getEnv("KAFKA_GROUP_ID", "image-processor-go"),
		KafkaInputTopic:         getEnv("KAFKA_TOPIC_INPUT", "image.processing.requested"),
		KafkaOutputTopic:        getEnv("KAFKA_TOPIC_OUTPUT", "image.processing.completed"),
		KafkaDocInput:           getEnv("KAFKA_TOPIC_DOC_INPUT", "document.processing.requested"),
		KafkaDocOutput:          getEnv("KAFKA_TOPIC_DOC_OUTPUT", "document.processing.completed"),
		KafkaStartFromBeginning: getEnvBool("KAFKA_START_FROM_BEGINNING", false),
		KafkaNumPartitions:      getEnvInt("KAFKA_NUM_PARTITIONS", 1),
		KafkaReplicationFactor:  getEnvInt("KAFKA_REPLICATION_FACTOR", 1),
		StorageEndpoint:         getEnv("STORAGE_ENDPOINT", "localhost:9000"),
		StorageAccessKey:        getEnv("STORAGE_ACCESS_KEY", ""),
		StorageSecretKey:        getEnv("STORAGE_SECRET_KEY", ""),
		StorageSSL:              getEnvBool("STORAGE_SSL", false),
		VipsMaxCache:            getEnvInt("VIPS_MAX_CACHE", 0),
		VipsMaxCacheMem:         getEnvInt("VIPS_MAX_CACHE_MEM", 0),
		WebPQuality:             getEnvInt("WEBP_QUALITY", 80),
		MaxImageTasks:           maxImageTasks,
		MaxDocumentTasks:        maxDocumentTasks,
		ProcessTimeout:          getEnvDuration("PROCESS_TIMEOUT", 120*time.Second),
		KafkaDLQSuffix:          getEnv("KAFKA_DLQ_SUFFIX", ".dlq"),
		MaxDeliveryTries:        getEnvInt("KAFKA_MAX_DELIVERY_TRIES", 3),
		RetryBackoff:            getEnvDuration("KAFKA_RETRY_BACKOFF", 500*time.Millisecond),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if value, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return fallback
}

func InitVips(cfg *Config) {
	bimg.VipsCacheSetMax(cfg.VipsMaxCache)
	bimg.VipsCacheSetMaxMem(cfg.VipsMaxCacheMem)

	slog.Info("libvips initialized", "version", bimg.VipsVersion)
}

func ShutdownVips() {
	bimg.Shutdown()
}
