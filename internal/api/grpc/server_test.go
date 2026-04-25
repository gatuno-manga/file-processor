package grpc

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/luis/file-processor/internal/api/grpc/pb"
	"github.com/luis/file-processor/internal/pool"
	"github.com/luis/file-processor/internal/processor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func mockProcess(data []byte, quality int, isBackfill bool) ([]byte, *processor.Metadata, error) {
	time.Sleep(50 * time.Millisecond)
	return append([]byte("processed: "), data...), &processor.Metadata{SizeBytes: len(data) + 11}, nil
}

func init() {
	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterImageProcessorServer(s, NewServer())
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()

	pool.InitPool(1)
	pool.SetProcessFunc(mockProcess)
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func TestProcess_Success(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewImageProcessorClient(conn)

	resp, err := client.Process(ctx, &pb.ProcessRequest{Data: []byte("test image data")})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if resp == nil {
		t.Fatal("Response is nil")
	}
}

func TestProcess_EmptyRequest(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewImageProcessorClient(conn)

	_, err = client.Process(ctx, &pb.ProcessRequest{Data: nil})
	if err == nil {
		t.Fatal("Expected error for empty request data, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument, got %v", err)
	}
}

func TestProcess_DeadlineExceeded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewImageProcessorClient(conn)

	_, err = client.Process(ctx, &pb.ProcessRequest{Data: []byte("test image data")})
	if err == nil {
		t.Fatal("Expected DeadlineExceeded, got nil")
	}

	st, ok := status.FromError(err)
	if !ok || (st.Code() != codes.DeadlineExceeded && st.Code() != codes.Canceled) {
		t.Errorf("Expected DeadlineExceeded or Canceled, got %v", err)
	}
}
