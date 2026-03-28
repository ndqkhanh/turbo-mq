package turbomq.network;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import turbomq.common.PartitionId;
import turbomq.network.proto.*;
import turbomq.storage.PartitionStore;
import turbomq.storage.PartitionStoreManager;

/**
 * gRPC ProducerService implementation.
 *
 * Accepts produce requests and appends messages to the appropriate
 * partition store. In a full implementation, this would route through
 * the Raft leader for the target partition.
 */
public final class ProducerServiceImpl extends ProducerServiceGrpc.ProducerServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(ProducerServiceImpl.class);

    private final PartitionStoreManager storeManager;

    public ProducerServiceImpl(PartitionStoreManager storeManager) {
        this.storeManager = storeManager;
    }

    @Override
    public void produce(ProduceRequest request, StreamObserver<ProduceResponse> responseObserver) {
        try {
            PartitionStore store = storeManager.getOrCreatePartition(
                    PartitionId.of(request.getPartition()));

            long nextOffset = store.latestOffset() + 1;
            store.append(nextOffset, request.getValue().toByteArray());

            log.debug("Produced message to partition {} at offset {}",
                    request.getPartition(), nextOffset);

            responseObserver.onNext(ProduceResponse.newBuilder()
                    .setSuccess(true)
                    .setOffset(nextOffset)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Failed to produce message", e);
            responseObserver.onNext(ProduceResponse.newBuilder()
                    .setSuccess(false)
                    .setError(e.getMessage())
                    .build());
            responseObserver.onCompleted();
        }
    }
}
