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
	// 1. Load Configuration
	cfg := processor.LoadConfig()

	// 2. Initialize Structured Logging
	var handler slog.Handler
	if cfg.AppEnv == "production" {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, nil)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	slog.Info("Gatuno File Processor starting...", "env", cfg.AppEnv)

	// 3. Initialize Processor (libvips)
	processor.InitVips(cfg)
	defer processor.ShutdownVips()
	slog.Info("Vips initialized successfully")

	// 4. Initialize Worker Pool
	pool.InitPool(cfg.PoolSize)
	slog.Info("Worker pool initialized", "size", cfg.PoolSize)

	// 5. Setup Context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		slog.Info("Shutdown signal received...")
		cancel()
	}()

	gGroup, ctx := errgroup.WithContext(ctx)

	// 6. Initialize Adapters for Async Flow
	s3Adapter, err := storage.NewS3Adapter(cfg.StorageEndpoint, cfg.StorageAccessKey, cfg.StorageSecretKey, cfg.StorageSSL)
	if err != nil {
		slog.Error("failed to create s3 adapter", "error", err)
		os.Exit(1)
	}

	kafkaAdapter := kafka.NewKafkaAdapter(cfg.KafkaBrokers, cfg.KafkaInputTopic, cfg.KafkaOutputTopic)
	kafkaOrchestrator := orchestrator.NewKafkaOrchestrator(s3Adapter, kafkaAdapter)

	// 7. Run Kafka Orchestrator
	gGroup.Go(func() error {
		slog.Info("Starting Kafka orchestrator")
		if err := kafkaOrchestrator.Run(ctx, kafkaAdapter); err != nil {
			return fmt.Errorf("kafka orchestrator error: %w", err)
		}
		return nil
	})

	// 8. Run Health and Metrics Server
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "OK")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if pool.IsReady() && kafkaAdapter.IsReady() {
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

	// 9. Setup and run gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.Port))
	if err != nil {
		slog.Error("failed to listen on port", "port", cfg.Port, "error", err)
		os.Exit(1)
	}

	grpcServer := g.NewServer()
	pb.RegisterImageProcessorServer(grpcServer, grpc.NewServer())

	gGroup.Go(func() error {
		slog.Info("gRPC server listening", "port", cfg.Port)
		// We use a separate goroutine to stop the server when the context is cancelled
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

	// 10. Wait for all components to finish
	if err := gGroup.Wait(); err != nil {
		slog.Error("Gatuno execution error", "error", err)
	}

	// 11. Post-wait cleanup
	slog.Info("Shutting down worker pool...")
	pool.Shutdown()

	slog.Info("Gatuno stopped")
}
