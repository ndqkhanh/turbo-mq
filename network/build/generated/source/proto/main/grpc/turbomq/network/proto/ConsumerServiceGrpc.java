package turbomq.network.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.62.2)",
    comments = "Source: turbomq/consumer.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ConsumerServiceGrpc {

  private ConsumerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "turbomq.ConsumerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<turbomq.network.proto.FetchRequest,
      turbomq.network.proto.FetchResponse> getFetchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Fetch",
      requestType = turbomq.network.proto.FetchRequest.class,
      responseType = turbomq.network.proto.FetchResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<turbomq.network.proto.FetchRequest,
      turbomq.network.proto.FetchResponse> getFetchMethod() {
    io.grpc.MethodDescriptor<turbomq.network.proto.FetchRequest, turbomq.network.proto.FetchResponse> getFetchMethod;
    if ((getFetchMethod = ConsumerServiceGrpc.getFetchMethod) == null) {
      synchronized (ConsumerServiceGrpc.class) {
        if ((getFetchMethod = ConsumerServiceGrpc.getFetchMethod) == null) {
          ConsumerServiceGrpc.getFetchMethod = getFetchMethod =
              io.grpc.MethodDescriptor.<turbomq.network.proto.FetchRequest, turbomq.network.proto.FetchResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Fetch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  turbomq.network.proto.FetchRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  turbomq.network.proto.FetchResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConsumerServiceMethodDescriptorSupplier("Fetch"))
              .build();
        }
      }
    }
    return getFetchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<turbomq.network.proto.CommitOffsetRequest,
      turbomq.network.proto.CommitOffsetResponse> getCommitOffsetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CommitOffset",
      requestType = turbomq.network.proto.CommitOffsetRequest.class,
      responseType = turbomq.network.proto.CommitOffsetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<turbomq.network.proto.CommitOffsetRequest,
      turbomq.network.proto.CommitOffsetResponse> getCommitOffsetMethod() {
    io.grpc.MethodDescriptor<turbomq.network.proto.CommitOffsetRequest, turbomq.network.proto.CommitOffsetResponse> getCommitOffsetMethod;
    if ((getCommitOffsetMethod = ConsumerServiceGrpc.getCommitOffsetMethod) == null) {
      synchronized (ConsumerServiceGrpc.class) {
        if ((getCommitOffsetMethod = ConsumerServiceGrpc.getCommitOffsetMethod) == null) {
          ConsumerServiceGrpc.getCommitOffsetMethod = getCommitOffsetMethod =
              io.grpc.MethodDescriptor.<turbomq.network.proto.CommitOffsetRequest, turbomq.network.proto.CommitOffsetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CommitOffset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  turbomq.network.proto.CommitOffsetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  turbomq.network.proto.CommitOffsetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConsumerServiceMethodDescriptorSupplier("CommitOffset"))
              .build();
        }
      }
    }
    return getCommitOffsetMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ConsumerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceStub>() {
        @java.lang.Override
        public ConsumerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConsumerServiceStub(channel, callOptions);
        }
      };
    return ConsumerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ConsumerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceBlockingStub>() {
        @java.lang.Override
        public ConsumerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConsumerServiceBlockingStub(channel, callOptions);
        }
      };
    return ConsumerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ConsumerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConsumerServiceFutureStub>() {
        @java.lang.Override
        public ConsumerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConsumerServiceFutureStub(channel, callOptions);
        }
      };
    return ConsumerServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void fetch(turbomq.network.proto.FetchRequest request,
        io.grpc.stub.StreamObserver<turbomq.network.proto.FetchResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getFetchMethod(), responseObserver);
    }

    /**
     */
    default void commitOffset(turbomq.network.proto.CommitOffsetRequest request,
        io.grpc.stub.StreamObserver<turbomq.network.proto.CommitOffsetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitOffsetMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ConsumerService.
   */
  public static abstract class ConsumerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ConsumerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ConsumerService.
   */
  public static final class ConsumerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ConsumerServiceStub> {
    private ConsumerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConsumerServiceStub(channel, callOptions);
    }

    /**
     */
    public void fetch(turbomq.network.proto.FetchRequest request,
        io.grpc.stub.StreamObserver<turbomq.network.proto.FetchResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getFetchMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void commitOffset(turbomq.network.proto.CommitOffsetRequest request,
        io.grpc.stub.StreamObserver<turbomq.network.proto.CommitOffsetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitOffsetMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ConsumerService.
   */
  public static final class ConsumerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ConsumerServiceBlockingStub> {
    private ConsumerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConsumerServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public turbomq.network.proto.FetchResponse fetch(turbomq.network.proto.FetchRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getFetchMethod(), getCallOptions(), request);
    }

    /**
     */
    public turbomq.network.proto.CommitOffsetResponse commitOffset(turbomq.network.proto.CommitOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitOffsetMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ConsumerService.
   */
  public static final class ConsumerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ConsumerServiceFutureStub> {
    private ConsumerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConsumerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConsumerServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<turbomq.network.proto.FetchResponse> fetch(
        turbomq.network.proto.FetchRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getFetchMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<turbomq.network.proto.CommitOffsetResponse> commitOffset(
        turbomq.network.proto.CommitOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitOffsetMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_FETCH = 0;
  private static final int METHODID_COMMIT_OFFSET = 1;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_FETCH:
          serviceImpl.fetch((turbomq.network.proto.FetchRequest) request,
              (io.grpc.stub.StreamObserver<turbomq.network.proto.FetchResponse>) responseObserver);
          break;
        case METHODID_COMMIT_OFFSET:
          serviceImpl.commitOffset((turbomq.network.proto.CommitOffsetRequest) request,
              (io.grpc.stub.StreamObserver<turbomq.network.proto.CommitOffsetResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getFetchMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              turbomq.network.proto.FetchRequest,
              turbomq.network.proto.FetchResponse>(
                service, METHODID_FETCH)))
        .addMethod(
          getCommitOffsetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              turbomq.network.proto.CommitOffsetRequest,
              turbomq.network.proto.CommitOffsetResponse>(
                service, METHODID_COMMIT_OFFSET)))
        .build();
  }

  private static abstract class ConsumerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ConsumerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return turbomq.network.proto.ConsumerProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ConsumerService");
    }
  }

  private static final class ConsumerServiceFileDescriptorSupplier
      extends ConsumerServiceBaseDescriptorSupplier {
    ConsumerServiceFileDescriptorSupplier() {}
  }

  private static final class ConsumerServiceMethodDescriptorSupplier
      extends ConsumerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ConsumerServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ConsumerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ConsumerServiceFileDescriptorSupplier())
              .addMethod(getFetchMethod())
              .addMethod(getCommitOffsetMethod())
              .build();
        }
      }
    }
    return result;
  }
}
