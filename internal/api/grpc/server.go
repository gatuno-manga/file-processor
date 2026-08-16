package grpc

import (
	"context"

	"github.com/luis/file-processor/internal/api/grpc/pb"
	"github.com/luis/file-processor/internal/pool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// Server implements the ImageProcessor gRPC service.
type Server struct {
	pb.UnimplementedImageProcessorServer
	pool *pool.WorkerPool
}

// NewServer creates a new instance of the Server.
func NewServer(p *pool.WorkerPool) *Server {
	return &Server{pool: p}
}

// RegisterHealthServer registers the gRPC health service on the given server.
func RegisterHealthServer(s *grpc.Server) {
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s, hs)
}

// Process handles an incoming image processing request.
func (s *Server) Process(ctx context.Context, req *pb.ProcessRequest) (*pb.ProcessResponse, error) {
	if req == nil || len(req.GetData()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty request data")
	}

	widths := make([]int, len(req.GetWidths()))
	for i, w := range req.GetWidths() {
		widths[i] = int(w)
	}

	results, err := s.pool.Submit(ctx, req.GetData(), false, widths)
	if err != nil {
		if err == context.DeadlineExceeded {
			return nil, status.Error(codes.DeadlineExceeded, "processing deadline exceeded")
		}
		if err == context.Canceled {
			return nil, status.Error(codes.Canceled, "request canceled")
		}
		return nil, status.Errorf(codes.Internal, "internal processing error: %v", err)
	}

	if len(results) == 0 {
		return nil, status.Error(codes.Internal, "no results generated")
	}

	pbResults := make([]*pb.ProcessedImage, len(results))
	for i, res := range results {
		width := 0
		if res.Metadata != nil {
			width = res.Metadata.Width
		}
		pbResults[i] = &pb.ProcessedImage{
			Data:  res.Data,
			Kind:  res.Kind,
			Width: int32(width),
		}
	}

	return &pb.ProcessResponse{
		// Deprecated field, kept for one release so pre-existing callers that
		// only read Data still get the primary/original image.
		Data:    results[0].Data,
		Results: pbResults,
	}, nil
}
