package processor

import (
	"os"
	"testing"
)

func TestLoadConfig_Defaults(t *testing.T) {
	os.Clearenv()
	cfg := LoadConfig()

	if cfg.AppEnv != "development" {
		t.Errorf("expected AppEnv to be development, got %s", cfg.AppEnv)
	}
	if cfg.Port != "50051" {
		t.Errorf("expected Port to be 50051, got %s", cfg.Port)
	}
	if cfg.HealthPort != "8081" {
		t.Errorf("expected HealthPort to be 8081, got %s", cfg.HealthPort)
	}
}

func TestLoadConfig_Overrides(t *testing.T) {
	os.Setenv("APP_ENV", "production")
	os.Setenv("GRPC_PORT", "9090")
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092")

	cfg := LoadConfig()

	if cfg.AppEnv != "production" {
		t.Errorf("expected AppEnv to be production, got %s", cfg.AppEnv)
	}
	if cfg.Port != "9090" {
		t.Errorf("expected Port to be 9090, got %s", cfg.Port)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "broker1:9092" {
		t.Errorf("expected KafkaBrokers to be overridden correctly")
	}
}
