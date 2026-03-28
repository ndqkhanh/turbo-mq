package turbomq.network;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.storage.PartitionStoreManager;

import java.io.IOException;
import java.util.concurrent.Executors;

/**
 * TurboMQ gRPC server hosting Producer, Consumer, and Admin services.
 *
 * Uses Virtual Threads for request handling to maximize throughput
 * with minimal thread overhead.
 */
public final class GrpcServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GrpcServer.class);

    private final int port;
    private final PartitionStoreManager storeManager;
    private Server server;

    public GrpcServer(int port, PartitionStoreManager storeManager) {
        this.port = port;
        this.storeManager = storeManager;
    }

    public void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ProducerServiceImpl(storeManager))
                .addService(new ConsumerServiceImpl(storeManager))
                .build()
                .start();

        log.info("TurboMQ gRPC server started on port {}", port);
    }

    public int getPort() {
        return server != null ? server.getPort() : port;
    }

    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdownNow();
            log.info("TurboMQ gRPC server stopped");
        }
    }
}
