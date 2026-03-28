package turbomq.network;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
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
 * Tests for the ConsumerService gRPC implementation.
 */
class ConsumerServiceImplTest {

    @TempDir
    Path tempDir;

    private PartitionStoreManager storeManager;
    private io.grpc.Server server;
    private ManagedChannel channel;
    private ConsumerServiceGrpc.ConsumerServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws Exception {
        storeManager = new PartitionStoreManager(tempDir);
        storeManager.open();

        PartitionStore store = storeManager.getOrCreatePartition(PartitionId.of(0));
        // Pre-populate messages
        store.append(1, "msg-1".getBytes());
        store.append(2, "msg-2".getBytes());
        store.append(3, "msg-3".getBytes());

        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new ConsumerServiceImpl(storeManager))
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        stub = ConsumerServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
        storeManager.close();
    }

    @Test
    void fetchMessagesFromOffset() {
        FetchResponse response = stub.fetch(FetchRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setOffset(1)
                .setMaxMessages(10)
                .build());

        assertThat(response.getMessagesList()).hasSize(3);
        assertThat(response.getMessages(0).getOffset()).isEqualTo(1);
        assertThat(response.getMessages(2).getOffset()).isEqualTo(3);
    }

    @Test
    void fetchWithMaxMessagesLimit() {
        FetchResponse response = stub.fetch(FetchRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setOffset(1)
                .setMaxMessages(2)
                .build());

        assertThat(response.getMessagesList()).hasSize(2);
        assertThat(response.getNextOffset()).isEqualTo(3);
    }

    @Test
    void fetchFromEndReturnsEmpty() {
        FetchResponse response = stub.fetch(FetchRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setOffset(4)
                .setMaxMessages(10)
                .build());

        assertThat(response.getMessagesList()).isEmpty();
    }

    @Test
    void commitAndFetchFromConsumerOffset() {
        // Commit offset for group
        CommitOffsetResponse commitResp = stub.commitOffset(CommitOffsetRequest.newBuilder()
                .setTopic("orders")
                .setPartition(0)
                .setConsumerGroup("group-1")
                .setOffset(2)
                .build());

        assertThat(commitResp.getSuccess()).isTrue();

        // Verify offset was stored
        PartitionStore store = storeManager.getOrCreatePartition(PartitionId.of(0));
        assertThat(store.getConsumerOffset("group-1")).isEqualTo(2);
    }
}
