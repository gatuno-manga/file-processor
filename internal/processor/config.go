package processor

import (
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/h2non/bimg"
)

// Config holds all configuration for the application.
type Config struct {
	AppEnv           string
	Port             string
	HealthPort       string
	PoolSize         int
	KafkaBrokers     []string
	KafkaGroupID     string
	KafkaInputTopic  string
	KafkaOutputTopic string
	StorageEndpoint  string
	StorageAccessKey string
	StorageSecretKey string
	StorageSSL       bool
	VipsMaxCache     int
	VipsMaxCacheMem  int
	WebPQuality      int
	MaxConcurrentTasks int
}

// LoadConfig reads configuration from environment variables with sensible defaults.
func LoadConfig() *Config {
	poolSize := getEnvInt("WORKER_POOL_SIZE", 0)
	maxConcurrentTasks := getEnvInt("MAX_CONCURRENT_TASKS", poolSize*2)
	if maxConcurrentTasks <= 0 {
		maxConcurrentTasks = 16
	}

	return &Config{
		AppEnv:           getEnv("APP_ENV", "development"),
		Port:             getEnv("GRPC_PORT", "50051"),
		HealthPort:       getEnv("HEALTH_PORT", "8081"),
		PoolSize:         poolSize,
		KafkaBrokers:     strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "file-processor-group"),
		KafkaInputTopic:  getEnv("KAFKA_TOPIC_INPUT", "image.processing.requested"),
		KafkaOutputTopic: getEnv("KAFKA_TOPIC_OUTPUT", "image.processing.completed"),
		StorageEndpoint:  getEnv("STORAGE_ENDPOINT", "localhost:9000"),
		StorageAccessKey: getEnv("STORAGE_ACCESS_KEY", ""),
		StorageSecretKey: getEnv("STORAGE_SECRET_KEY", ""),
		StorageSSL:       getEnvBool("STORAGE_SSL", false),
		VipsMaxCache:     getEnvInt("VIPS_MAX_CACHE", 0),
		VipsMaxCacheMem:  getEnvInt("VIPS_MAX_CACHE_MEM", 0),
		WebPQuality:      getEnvInt("WEBP_QUALITY", 80),
		MaxConcurrentTasks: maxConcurrentTasks,
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

// InitVips initializes the libvips engine with specific cache limits.
func InitVips(cfg *Config) {
	bimg.VipsCacheSetMax(cfg.VipsMaxCache)
	bimg.VipsCacheSetMaxMem(cfg.VipsMaxCacheMem)

	slog.Info("libvips initialized", "version", bimg.VipsVersion)
}

// ShutdownVips shuts down the libvips engine.
func ShutdownVips() {
	bimg.Shutdown()
}
