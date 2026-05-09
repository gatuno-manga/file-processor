package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/luis/file-processor/internal/adapter/kafka"
	"github.com/luis/file-processor/internal/adapter/storage"
	"github.com/luis/file-processor/internal/api/grpc"
	"github.com/luis/file-processor/internal/api/grpc/pb"
	"github.com/luis/file-processor/internal/orchestrator"
	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/processor"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	g "google.golang.org/grpc"
)

func main() {
	cfg := processor.LoadConfig()

	var handler slog.Handler
	if cfg.AppEnv == "production" {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, nil)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	slog.Info("Gatuno File Processor starting...", "env", cfg.AppEnv)
	slog.Info("Hot Reload Test: Air is working perfectly! 🚀")

	processor.InitVips(cfg)
	processor.DefaultQuality = cfg.WebPQuality
	defer processor.ShutdownVips()
	slog.Info("Vips initialized successfully", "quality", cfg.WebPQuality)

	workerPool := pool.NewWorkerPool(cfg.PoolSize)
	defer workerPool.Shutdown()
	slog.Info("Worker pool initialized", "size", cfg.PoolSize)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		slog.Info("Shutdown signal received...")
		cancel()
	}()

	gGroup, ctx := errgroup.WithContext(ctx)

	s3Adapter, err := storage.NewS3Adapter(cfg.StorageEndpoint, cfg.StorageAccessKey, cfg.StorageSecretKey, cfg.StorageSSL)
	if err != nil {
		slog.Error("failed to create s3 adapter", "error", err)
		os.Exit(1)
	}
	if err := s3Adapter.Ping(ctx); err != nil {
		slog.Error("failed to connect to S3", "endpoint", cfg.StorageEndpoint, "error", err)
		os.Exit(1)
	}
	slog.Info("Successfully connected to S3", "endpoint", cfg.StorageEndpoint)

	kafkaAdapter := kafka.NewKafkaAdapter(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaInputTopic, cfg.KafkaOutputTopic, cfg.MaxConcurrentTasks)
	if err := kafkaAdapter.Ping(ctx); err != nil {
		slog.Error("failed to connect to Kafka", "brokers", cfg.KafkaBrokers, "error", err)
		os.Exit(1)
	}
	slog.Info("Successfully connected to Kafka", "brokers", cfg.KafkaBrokers)

	kafkaOrchestrator := orchestrator.NewKafkaOrchestrator(s3Adapter, kafkaAdapter, workerPool)

	gGroup.Go(func() error {
		slog.Info("Starting Kafka orchestrator")
		if err := kafkaOrchestrator.Run(ctx, kafkaAdapter); err != nil {
			return fmt.Errorf("kafka orchestrator error: %w", err)
		}
		return nil
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if kafkaAdapter.IsReady() {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "READY")
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintln(w, "NOT READY")
		}
	})
	mux.Handle("/metrics", promhttp.Handler())

	healthServer := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.HealthPort),
		Handler: mux,
	}

	gGroup.Go(func() error {
		slog.Info("Health server listening", "port", cfg.HealthPort)
		go func() {
			<-ctx.Done()
			slog.Info("Shutting down health server...")
			healthServer.Shutdown(context.Background())
		}()
		if err := healthServer.ListenAndServe(); err != http.ErrServerClosed {
			return fmt.Errorf("health server error: %w", err)
		}
		return nil
	})

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.Port))
	if err != nil {
		slog.Error("failed to listen on port", "port", cfg.Port, "error", err)
		os.Exit(1)
	}

	grpcServer := g.NewServer()
	pb.RegisterImageProcessorServer(grpcServer, grpc.NewServer(workerPool))

	gGroup.Go(func() error {
		slog.Info("gRPC server listening", "port", cfg.Port)
		go func() {
			<-ctx.Done()
			slog.Info("Shutting down gRPC server...")
			grpcServer.GracefulStop()
		}()
		if err := grpcServer.Serve(lis); err != nil && err != g.ErrServerStopped {
			return fmt.Errorf("grpc server error: %w", err)
		}
		return nil
	})

	if err := gGroup.Wait(); err != nil {
		slog.Error("Gatuno execution error", "error", err)
	}

	slog.Info("Gatuno stopped")
}
