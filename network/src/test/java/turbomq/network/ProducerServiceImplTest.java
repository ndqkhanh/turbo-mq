package turbomq.network;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import turbomq.common.PartitionId;
import turbomq.network.proto.*;
import turbomq.storage.PartitionStore;
import turbomq.storage.PartitionStoreManager;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for the ProducerService gRPC implementation.
 * Uses in-process transport for fast, no-network testing.
 */
class ProducerServiceImplTest {

    @TempDir
    Path tempDir;

    private PartitionStoreManager storeManager;
    private io.grpc.Server server;
    private ManagedChannel channel;
    private ProducerServiceGrpc.ProducerServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws Exception {
        storeManager = new PartitionStoreManager(tempDir);
        storeManager.open();

        // Pre-create partition 0
        storeManager.getOrCreatePartition(PartitionId.of(0));

        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new ProducerServiceImpl(storeManager))
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        stub = ProducerServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
        storeManager.close();
    }

    @Test
    void produceMessageSuccessfully() {
        ProduceResponse response = stub.produce(ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setKey(ByteString.copyFromUtf8("key1"))
                .setValue(ByteString.copyFromUtf8("value1"))
                .build());

        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getOffset()).isEqualTo(1);
    }

    @Test
    void produceMultipleMessagesIncrementsOffset() {
        stub.produce(ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setValue(ByteString.copyFromUtf8("msg1"))
                .build());

        ProduceResponse response = stub.produce(ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setValue(ByteString.copyFromUtf8("msg2"))
                .build());

        assertThat(response.getOffset()).isEqualTo(2);
    }

    @Test
    void producedMessageIsReadableFromStore() {
        stub.produce(ProduceRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setValue(ByteString.copyFromUtf8("stored-value"))
                .build());

        PartitionStore store = storeManager.getOrCreatePartition(PartitionId.of(0));
        byte[] data = store.read(1);
        assertThat(data).isNotNull();
    }
}
