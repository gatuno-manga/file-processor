package grpc

import (
	"bytes"
	"context"
	"fmt"
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

var (
	lis      *bufconn.Listener
	testPool *pool.WorkerPool
)

func mockProcess(data []byte, cfg processor.ImageConfig, isBackfill bool, widths []int) ([]processor.ProcessedResult, error) {
	time.Sleep(50 * time.Millisecond)
	results := []processor.ProcessedResult{
		{
			Data:     append([]byte("processed: "), data...),
			Metadata: &processor.Metadata{SizeBytes: len(data) + 11},
			Kind:     processor.KindOriginal,
		},
	}
	for _, w := range widths {
		results = append(results, processor.ProcessedResult{
			Data:     []byte(fmt.Sprintf("variant-%d", w)),
			Metadata: &processor.Metadata{Width: w},
			Kind:     processor.KindVariant,
		})
	}
	return results, nil
}

func init() {
	testPool = pool.NewWorkerPool(1, processor.DefaultConfig)
	testPool.SetProcessFunc(mockProcess)

	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer()
	pb.RegisterImageProcessorServer(s, NewServer(testPool))
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
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

// TestProcess_WithVariants covers the breaking proto change needed to return
// more than one output image over gRPC: ProcessResponse.results must carry
// every rendition with its Kind/Width, and the deprecated Data field must
// keep mirroring the original for callers that haven't migrated yet.
func TestProcess_WithVariants(t *testing.T) {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	defer conn.Close()
	client := pb.NewImageProcessorClient(conn)

	resp, err := client.Process(ctx, &pb.ProcessRequest{Data: []byte("test image data"), Widths: []int32{600, 300}})
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	if len(resp.GetResults()) != 3 {
		t.Fatalf("expected 3 results (1 original + 2 variants), got %d", len(resp.GetResults()))
	}
	if resp.GetResults()[0].GetKind() != processor.KindOriginal {
		t.Errorf("expected first result kind=%q, got %q", processor.KindOriginal, resp.GetResults()[0].GetKind())
	}
	if !bytes.Equal(resp.GetData(), resp.GetResults()[0].GetData()) {
		t.Error("expected the deprecated Data field to mirror the original result for backward compatibility")
	}

	wantWidths := []int32{600, 300}
	for i, w := range wantWidths {
		r := resp.GetResults()[i+1]
		if r.GetKind() != processor.KindVariant {
			t.Errorf("variant %d: expected kind=%q, got %q", i, processor.KindVariant, r.GetKind())
		}
		if r.GetWidth() != w {
			t.Errorf("variant %d: expected width %d, got %d", i, w, r.GetWidth())
		}
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
