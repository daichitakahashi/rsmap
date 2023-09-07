// Code generated by protoc-gen-connect-go. DO NOT EDIT.
//
// Source: resource_map/v1/resource_map.proto

package resource_mapv1connect

import (
	context "context"
	errors "errors"
	connect_go "github.com/bufbuild/connect-go"
	v1 "github.com/daichitakahashi/rsmap/internal/gen/resource_map/v1"
	http "net/http"
	strings "strings"
)

// This is a compile-time assertion to ensure that this generated file and the connect package are
// compatible. If you get a compiler error that this constant is not defined, this code was
// generated with a version of connect newer than the one compiled into your binary. You can fix the
// problem by either regenerating this code with an older version of connect or updating the connect
// version compiled into your binary.
const _ = connect_go.IsAtLeastVersion0_1_0

const (
	// ResourceMapServiceName is the fully-qualified name of the ResourceMapService service.
	ResourceMapServiceName = "internal.proto.resource_map.v1.ResourceMapService"
)

// These constants are the fully-qualified names of the RPCs defined in this package. They're
// exposed at runtime as Spec.Procedure and as the final two segments of the HTTP route.
//
// Note that these are different from the fully-qualified method names used by
// google.golang.org/protobuf/reflect/protoreflect. To convert from these constants to
// reflection-formatted method names, remove the leading slash and convert the remaining slash to a
// period.
const (
	// ResourceMapServiceTryInitResourceProcedure is the fully-qualified name of the
	// ResourceMapService's TryInitResource RPC.
	ResourceMapServiceTryInitResourceProcedure = "/internal.proto.resource_map.v1.ResourceMapService/TryInitResource"
	// ResourceMapServiceCompleteInitResourceProcedure is the fully-qualified name of the
	// ResourceMapService's CompleteInitResource RPC.
	ResourceMapServiceCompleteInitResourceProcedure = "/internal.proto.resource_map.v1.ResourceMapService/CompleteInitResource"
	// ResourceMapServiceAcquireProcedure is the fully-qualified name of the ResourceMapService's
	// Acquire RPC.
	ResourceMapServiceAcquireProcedure = "/internal.proto.resource_map.v1.ResourceMapService/Acquire"
	// ResourceMapServiceReleaseProcedure is the fully-qualified name of the ResourceMapService's
	// Release RPC.
	ResourceMapServiceReleaseProcedure = "/internal.proto.resource_map.v1.ResourceMapService/Release"
)

// ResourceMapServiceClient is a client for the internal.proto.resource_map.v1.ResourceMapService
// service.
type ResourceMapServiceClient interface {
	TryInitResource(context.Context, *connect_go.Request[v1.TryInitResourceRequest]) (*connect_go.Response[v1.TryInitResourceResponse], error)
	CompleteInitResource(context.Context, *connect_go.Request[v1.CompleteInitResourceRequest]) (*connect_go.Response[v1.CompleteInitResourceResponse], error)
	Acquire(context.Context, *connect_go.Request[v1.AcquireRequest]) (*connect_go.Response[v1.AcquireResponse], error)
	Release(context.Context, *connect_go.Request[v1.ReleaseRequest]) (*connect_go.Response[v1.ReleaseResponse], error)
}

// NewResourceMapServiceClient constructs a client for the
// internal.proto.resource_map.v1.ResourceMapService service. By default, it uses the Connect
// protocol with the binary Protobuf Codec, asks for gzipped responses, and sends uncompressed
// requests. To use the gRPC or gRPC-Web protocols, supply the connect.WithGRPC() or
// connect.WithGRPCWeb() options.
//
// The URL supplied here should be the base URL for the Connect or gRPC server (for example,
// http://api.acme.com or https://acme.com/grpc).
func NewResourceMapServiceClient(httpClient connect_go.HTTPClient, baseURL string, opts ...connect_go.ClientOption) ResourceMapServiceClient {
	baseURL = strings.TrimRight(baseURL, "/")
	return &resourceMapServiceClient{
		tryInitResource: connect_go.NewClient[v1.TryInitResourceRequest, v1.TryInitResourceResponse](
			httpClient,
			baseURL+ResourceMapServiceTryInitResourceProcedure,
			opts...,
		),
		completeInitResource: connect_go.NewClient[v1.CompleteInitResourceRequest, v1.CompleteInitResourceResponse](
			httpClient,
			baseURL+ResourceMapServiceCompleteInitResourceProcedure,
			opts...,
		),
		acquire: connect_go.NewClient[v1.AcquireRequest, v1.AcquireResponse](
			httpClient,
			baseURL+ResourceMapServiceAcquireProcedure,
			opts...,
		),
		release: connect_go.NewClient[v1.ReleaseRequest, v1.ReleaseResponse](
			httpClient,
			baseURL+ResourceMapServiceReleaseProcedure,
			opts...,
		),
	}
}

// resourceMapServiceClient implements ResourceMapServiceClient.
type resourceMapServiceClient struct {
	tryInitResource      *connect_go.Client[v1.TryInitResourceRequest, v1.TryInitResourceResponse]
	completeInitResource *connect_go.Client[v1.CompleteInitResourceRequest, v1.CompleteInitResourceResponse]
	acquire              *connect_go.Client[v1.AcquireRequest, v1.AcquireResponse]
	release              *connect_go.Client[v1.ReleaseRequest, v1.ReleaseResponse]
}

// TryInitResource calls internal.proto.resource_map.v1.ResourceMapService.TryInitResource.
func (c *resourceMapServiceClient) TryInitResource(ctx context.Context, req *connect_go.Request[v1.TryInitResourceRequest]) (*connect_go.Response[v1.TryInitResourceResponse], error) {
	return c.tryInitResource.CallUnary(ctx, req)
}

// CompleteInitResource calls
// internal.proto.resource_map.v1.ResourceMapService.CompleteInitResource.
func (c *resourceMapServiceClient) CompleteInitResource(ctx context.Context, req *connect_go.Request[v1.CompleteInitResourceRequest]) (*connect_go.Response[v1.CompleteInitResourceResponse], error) {
	return c.completeInitResource.CallUnary(ctx, req)
}

// Acquire calls internal.proto.resource_map.v1.ResourceMapService.Acquire.
func (c *resourceMapServiceClient) Acquire(ctx context.Context, req *connect_go.Request[v1.AcquireRequest]) (*connect_go.Response[v1.AcquireResponse], error) {
	return c.acquire.CallUnary(ctx, req)
}

// Release calls internal.proto.resource_map.v1.ResourceMapService.Release.
func (c *resourceMapServiceClient) Release(ctx context.Context, req *connect_go.Request[v1.ReleaseRequest]) (*connect_go.Response[v1.ReleaseResponse], error) {
	return c.release.CallUnary(ctx, req)
}

// ResourceMapServiceHandler is an implementation of the
// internal.proto.resource_map.v1.ResourceMapService service.
type ResourceMapServiceHandler interface {
	TryInitResource(context.Context, *connect_go.Request[v1.TryInitResourceRequest]) (*connect_go.Response[v1.TryInitResourceResponse], error)
	CompleteInitResource(context.Context, *connect_go.Request[v1.CompleteInitResourceRequest]) (*connect_go.Response[v1.CompleteInitResourceResponse], error)
	Acquire(context.Context, *connect_go.Request[v1.AcquireRequest]) (*connect_go.Response[v1.AcquireResponse], error)
	Release(context.Context, *connect_go.Request[v1.ReleaseRequest]) (*connect_go.Response[v1.ReleaseResponse], error)
}

// NewResourceMapServiceHandler builds an HTTP handler from the service implementation. It returns
// the path on which to mount the handler and the handler itself.
//
// By default, handlers support the Connect, gRPC, and gRPC-Web protocols with the binary Protobuf
// and JSON codecs. They also support gzip compression.
func NewResourceMapServiceHandler(svc ResourceMapServiceHandler, opts ...connect_go.HandlerOption) (string, http.Handler) {
	resourceMapServiceTryInitResourceHandler := connect_go.NewUnaryHandler(
		ResourceMapServiceTryInitResourceProcedure,
		svc.TryInitResource,
		opts...,
	)
	resourceMapServiceCompleteInitResourceHandler := connect_go.NewUnaryHandler(
		ResourceMapServiceCompleteInitResourceProcedure,
		svc.CompleteInitResource,
		opts...,
	)
	resourceMapServiceAcquireHandler := connect_go.NewUnaryHandler(
		ResourceMapServiceAcquireProcedure,
		svc.Acquire,
		opts...,
	)
	resourceMapServiceReleaseHandler := connect_go.NewUnaryHandler(
		ResourceMapServiceReleaseProcedure,
		svc.Release,
		opts...,
	)
	return "/internal.proto.resource_map.v1.ResourceMapService/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case ResourceMapServiceTryInitResourceProcedure:
			resourceMapServiceTryInitResourceHandler.ServeHTTP(w, r)
		case ResourceMapServiceCompleteInitResourceProcedure:
			resourceMapServiceCompleteInitResourceHandler.ServeHTTP(w, r)
		case ResourceMapServiceAcquireProcedure:
			resourceMapServiceAcquireHandler.ServeHTTP(w, r)
		case ResourceMapServiceReleaseProcedure:
			resourceMapServiceReleaseHandler.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})
}

// UnimplementedResourceMapServiceHandler returns CodeUnimplemented from all methods.
type UnimplementedResourceMapServiceHandler struct{}

func (UnimplementedResourceMapServiceHandler) TryInitResource(context.Context, *connect_go.Request[v1.TryInitResourceRequest]) (*connect_go.Response[v1.TryInitResourceResponse], error) {
	return nil, connect_go.NewError(connect_go.CodeUnimplemented, errors.New("internal.proto.resource_map.v1.ResourceMapService.TryInitResource is not implemented"))
}

func (UnimplementedResourceMapServiceHandler) CompleteInitResource(context.Context, *connect_go.Request[v1.CompleteInitResourceRequest]) (*connect_go.Response[v1.CompleteInitResourceResponse], error) {
	return nil, connect_go.NewError(connect_go.CodeUnimplemented, errors.New("internal.proto.resource_map.v1.ResourceMapService.CompleteInitResource is not implemented"))
}

func (UnimplementedResourceMapServiceHandler) Acquire(context.Context, *connect_go.Request[v1.AcquireRequest]) (*connect_go.Response[v1.AcquireResponse], error) {
	return nil, connect_go.NewError(connect_go.CodeUnimplemented, errors.New("internal.proto.resource_map.v1.ResourceMapService.Acquire is not implemented"))
}

func (UnimplementedResourceMapServiceHandler) Release(context.Context, *connect_go.Request[v1.ReleaseRequest]) (*connect_go.Response[v1.ReleaseResponse], error) {
	return nil, connect_go.NewError(connect_go.CodeUnimplemented, errors.New("internal.proto.resource_map.v1.ResourceMapService.Release is not implemented"))
}
