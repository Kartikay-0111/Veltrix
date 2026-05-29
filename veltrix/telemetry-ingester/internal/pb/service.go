// Hand-written gRPC service definitions matching proto/telemetry.proto.
// The Dockerfile regenerates this with protoc during the build.

package pb

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ─── Service Registration ────────────────────────────────────────────────────

const TelemetryService_StreamTelemetry_FullMethodName = "/telemetry.TelemetryService/StreamTelemetry"

// TelemetryServiceServer is the server-side interface.
type TelemetryServiceServer interface {
	StreamTelemetry(TelemetryService_StreamTelemetryServer) error
	mustEmbedUnimplementedTelemetryServiceServer()
}

// UnimplementedTelemetryServiceServer must be embedded for forward compatibility.
type UnimplementedTelemetryServiceServer struct{}

func (UnimplementedTelemetryServiceServer) StreamTelemetry(TelemetryService_StreamTelemetryServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamTelemetry not implemented")
}
func (UnimplementedTelemetryServiceServer) mustEmbedUnimplementedTelemetryServiceServer() {}

// UnsafeTelemetryServiceServer may be embedded for opt-out of forward compat.
type UnsafeTelemetryServiceServer interface {
	mustEmbedUnimplementedTelemetryServiceServer()
}

// RegisterTelemetryServiceServer registers the service with a gRPC server.
func RegisterTelemetryServiceServer(s grpc.ServiceRegistrar, srv TelemetryServiceServer) {
	s.RegisterService(&TelemetryService_ServiceDesc, srv)
}

// TelemetryService_ServiceDesc is the gRPC service descriptor.
var TelemetryService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "telemetry.TelemetryService",
	HandlerType: (*TelemetryServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamTelemetry",
			Handler:       _TelemetryService_StreamTelemetry_Handler,
			ClientStreams:  true,
			ServerStreams:  false,
		},
	},
	Metadata: "proto/telemetry.proto",
}

// ─── Server-side stream handler ──────────────────────────────────────────────

type TelemetryService_StreamTelemetryServer interface {
	SendAndClose(*StreamTelemetryResponse) error
	Recv() (*AuditBatch, error)
	grpc.ServerStream
}

type telemetryServiceStreamTelemetryServer struct {
	grpc.ServerStream
}

func (x *telemetryServiceStreamTelemetryServer) SendAndClose(resp *StreamTelemetryResponse) error {
	return x.ServerStream.SendMsg(resp)
}

func (x *telemetryServiceStreamTelemetryServer) Recv() (*AuditBatch, error) {
	m := new(AuditBatch)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _TelemetryService_StreamTelemetry_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(TelemetryServiceServer).StreamTelemetry(&telemetryServiceStreamTelemetryServer{stream})
}

// ─── Client-side stub ────────────────────────────────────────────────────────

// TelemetryServiceClient is the client-side interface.
type TelemetryServiceClient interface {
	StreamTelemetry(ctx context.Context, opts ...grpc.CallOption) (TelemetryService_StreamTelemetryClient, error)
}

type telemetryServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewTelemetryServiceClient(cc grpc.ClientConnInterface) TelemetryServiceClient {
	return &telemetryServiceClient{cc}
}

func (c *telemetryServiceClient) StreamTelemetry(ctx context.Context, opts ...grpc.CallOption) (TelemetryService_StreamTelemetryClient, error) {
	stream, err := c.cc.NewStream(ctx, &TelemetryService_ServiceDesc.Streams[0], TelemetryService_StreamTelemetry_FullMethodName, opts...)
	if err != nil {
		return nil, err
	}
	x := &telemetryServiceStreamTelemetryClient{stream}
	return x, nil
}

type TelemetryService_StreamTelemetryClient interface {
	Send(*AuditBatch) error
	CloseAndRecv() (*StreamTelemetryResponse, error)
	grpc.ClientStream
}

type telemetryServiceStreamTelemetryClient struct {
	grpc.ClientStream
}

func (x *telemetryServiceStreamTelemetryClient) Send(m *AuditBatch) error {
	return x.ClientStream.SendMsg(m)
}

func (x *telemetryServiceStreamTelemetryClient) CloseAndRecv() (*StreamTelemetryResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(StreamTelemetryResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
