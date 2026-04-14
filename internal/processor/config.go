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
	KafkaInputTopic  string
	KafkaOutputTopic string
	StorageEndpoint  string
	StorageAccessKey string
	StorageSecretKey string
	StorageSSL       bool
	VipsMaxCache     int
	VipsMaxCacheMem  int
}

// LoadConfig reads configuration from environment variables with sensible defaults.
func LoadConfig() *Config {
	return &Config{
		AppEnv:           getEnv("APP_ENV", "development"),
		Port:             getEnv("GRPC_PORT", "50051"),
		HealthPort:       getEnv("HEALTH_PORT", "8081"),
		PoolSize:         getEnvInt("WORKER_POOL_SIZE", 0),
		KafkaBrokers:     strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaInputTopic:  getEnv("KAFKA_TOPIC_INPUT", "image.downloaded"),
		KafkaOutputTopic: getEnv("KAFKA_TOPIC_OUTPUT", "file.sanitized"),
		StorageEndpoint:  getEnv("STORAGE_ENDPOINT", "localhost:9000"),
		StorageAccessKey: getEnv("STORAGE_ACCESS_KEY", ""),
		StorageSecretKey: getEnv("STORAGE_SECRET_KEY", ""),
		StorageSSL:       getEnvBool("STORAGE_SSL", false),
		VipsMaxCache:     getEnvInt("VIPS_MAX_CACHE", 0),
		VipsMaxCacheMem:  getEnvInt("VIPS_MAX_CACHE_MEM", 0),
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
	// Set libvips cache limits to ensure predictable memory usage.
	// 0 means disabled/minimal.
	bimg.VipsCacheSetMax(cfg.VipsMaxCache)
	bimg.VipsCacheSetMaxMem(cfg.VipsMaxCacheMem)

	slog.Info("libvips initialized", "version", bimg.VipsVersion)
}

// ShutdownVips shuts down the libvips engine.
func ShutdownVips() {
	bimg.Shutdown()
}
