package turbomq.network;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.common.PartitionId;
import turbomq.network.proto.*;
import turbomq.storage.PartitionStore;
import turbomq.storage.PartitionStoreManager;

import java.util.List;

/**
 * gRPC ConsumerService implementation.
 *
 * Handles fetch requests by reading messages from the partition store
 * and consumer offset commit/read operations.
 */
public final class ConsumerServiceImpl extends ConsumerServiceGrpc.ConsumerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(ConsumerServiceImpl.class);

    private final PartitionStoreManager storeManager;

    public ConsumerServiceImpl(PartitionStoreManager storeManager) {
        this.storeManager = storeManager;
    }

    @Override
    public void fetch(FetchRequest request, StreamObserver<FetchResponse> responseObserver) {
        try {
            PartitionStore store = storeManager.getOrCreatePartition(
                    PartitionId.of(request.getPartition()));

            long fromOffset = request.getOffset();
            int maxMessages = request.getMaxMessages() > 0 ? request.getMaxMessages() : 100;
            long toOffset = fromOffset + maxMessages;

            // Cap to latest offset
            long latest = store.latestOffset();
            if (toOffset > latest + 1) {
                toOffset = latest + 1;
            }

            List<byte[]> messages = store.readRange(fromOffset, toOffset);

            FetchResponse.Builder builder = FetchResponse.newBuilder();
            for (int i = 0; i < messages.size(); i++) {
                builder.addMessages(Message.newBuilder()
                        .setOffset(fromOffset + i)
                        .setValue(com.google.protobuf.ByteString.copyFrom(messages.get(i)))
                        .build());
            }
            builder.setNextOffset(fromOffset + messages.size());

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to fetch messages", e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void commitOffset(CommitOffsetRequest request, StreamObserver<CommitOffsetResponse> responseObserver) {
        try {
            PartitionStore store = storeManager.getOrCreatePartition(
                    PartitionId.of(request.getPartition()));

            store.commitConsumerOffset(request.getConsumerGroup(), request.getOffset());

            responseObserver.onNext(CommitOffsetResponse.newBuilder()
                    .setSuccess(true)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to commit offset", e);
            responseObserver.onNext(CommitOffsetResponse.newBuilder()
                    .setSuccess(false)
                    .setError(e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }
}
