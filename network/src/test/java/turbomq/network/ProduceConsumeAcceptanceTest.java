package turbomq.network;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import turbomq.common.PartitionId;
import turbomq.network.proto.*;
import turbomq.storage.PartitionStoreManager;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end acceptance test: produce messages via gRPC,
 * consume them back, commit offsets, and verify round-trip.
 */
class ProduceConsumeAcceptanceTest {

    @TempDir
    Path tempDir;

    private PartitionStoreManager storeManager;
    private io.grpc.Server server;
    private ManagedChannel channel;
    private ProducerServiceGrpc.ProducerServiceBlockingStub producerStub;
    private ConsumerServiceGrpc.ConsumerServiceBlockingStub consumerStub;

    @BeforeEach
    void setUp() throws Exception {
        storeManager = new PartitionStoreManager(tempDir);
        storeManager.open();
        storeManager.getOrCreatePartition(PartitionId.of(0));
        storeManager.getOrCreatePartition(PartitionId.of(1));

        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new ProducerServiceImpl(storeManager))
                .addService(new ConsumerServiceImpl(storeManager))
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        producerStub = ProducerServiceGrpc.newBlockingStub(channel);
        consumerStub = ConsumerServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
        storeManager.close();
    }

    @Test
    void produceAndConsumeRoundTrip() {
        // Produce 3 messages
        for (int i = 1; i <= 3; i++) {
            ProduceResponse resp = producerStub.produce(ProduceRequest.newBuilder()
                    .setTopic("events")
                    .setPartition(0)
                    .setValue(ByteString.copyFromUtf8("event-" + i))
                    .build());
            assertThat(resp.getSuccess()).isTrue();
            assertThat(resp.getOffset()).isEqualTo(i);
        }

        // Consume all messages from offset 1
        FetchResponse fetch = consumerStub.fetch(FetchRequest.newBuilder()
                .setTopic("events")
                .setPartition(0)
                .setOffset(1)
                .setMaxMessages(10)
                .build());

        assertThat(fetch.getMessagesList()).hasSize(3);
        assertThat(fetch.getMessages(0).getValue().toStringUtf8()).isEqualTo("event-1");
        assertThat(fetch.getMessages(1).getValue().toStringUtf8()).isEqualTo("event-2");
        assertThat(fetch.getMessages(2).getValue().toStringUtf8()).isEqualTo("event-3");
        assertThat(fetch.getNextOffset()).isEqualTo(4);
    }

    @Test
    void consumeWithOffsetCommit() {
        // Produce messages
        producerStub.produce(ProduceRequest.newBuilder()
                .setTopic("orders").setPartition(0)
                .setValue(ByteString.copyFromUtf8("order-1")).build());
        producerStub.produce(ProduceRequest.newBuilder()
                .setTopic("orders").setPartition(0)
                .setValue(ByteString.copyFromUtf8("order-2")).build());

        // Consumer reads first message
        FetchResponse fetch1 = consumerStub.fetch(FetchRequest.newBuilder()
                .setTopic("orders").setPartition(0)
                .setOffset(1).setMaxMessages(1).build());
        assertThat(fetch1.getMessagesList()).hasSize(1);

        // Commit offset
        CommitOffsetResponse commit = consumerStub.commitOffset(CommitOffsetRequest.newBuilder()
                .setTopic("orders").setPartition(0)
                .setConsumerGroup("order-processor")
                .setOffset(fetch1.getNextOffset()).build());
        assertThat(commit.getSuccess()).isTrue();

        // Resume from committed offset
        FetchResponse fetch2 = consumerStub.fetch(FetchRequest.newBuilder()
                .setTopic("orders").setPartition(0)
                .setOffset(fetch1.getNextOffset()).setMaxMessages(10).build());
        assertThat(fetch2.getMessagesList()).hasSize(1);
        assertThat(fetch2.getMessages(0).getValue().toStringUtf8()).isEqualTo("order-2");
    }

    @Test
    void multiPartitionIsolation() {
        // Produce to partition 0
        producerStub.produce(ProduceRequest.newBuilder()
                .setTopic("t").setPartition(0)
                .setValue(ByteString.copyFromUtf8("p0-msg")).build());

        // Produce to partition 1
        producerStub.produce(ProduceRequest.newBuilder()
                .setTopic("t").setPartition(1)
                .setValue(ByteString.copyFromUtf8("p1-msg")).build());

        // Fetch from each partition independently
        FetchResponse f0 = consumerStub.fetch(FetchRequest.newBuilder()
                .setTopic("t").setPartition(0).setOffset(1).setMaxMessages(10).build());
        FetchResponse f1 = consumerStub.fetch(FetchRequest.newBuilder()
                .setTopic("t").setPartition(1).setOffset(1).setMaxMessages(10).build());

        assertThat(f0.getMessagesList()).hasSize(1);
        assertThat(f0.getMessages(0).getValue().toStringUtf8()).isEqualTo("p0-msg");

        assertThat(f1.getMessagesList()).hasSize(1);
        assertThat(f1.getMessages(0).getValue().toStringUtf8()).isEqualTo("p1-msg");
    }
}
