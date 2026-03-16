/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.metrics.event.PartitionOffsetLagMetricEvent;

/**
 * Creates {@link Partition} from {@link PartitionState},
 * retrieves offset for it
 */
public class PartitionFactory {

    private static final Logger LOGGER = getLogger(PartitionFactory.class);

    private final PartitionOffsetProvider partitionOffsetProvider;

    private final MetricsEventPublisher metricsEventPublisher;

    public PartitionFactory(PartitionOffsetProvider partitionOffsetProvider, MetricsEventPublisher metricsEventPublisher) {
        this.partitionOffsetProvider = partitionOffsetProvider;
        this.metricsEventPublisher = metricsEventPublisher;
    }

    public Partition initPartition(Timestamp startTime, Timestamp endTime) {
        Partition partition = Partition.builder()
                .token(InitialPartition.PARTITION_TOKEN)
                .parentTokens(Set.of())
                .startTimestamp(startTime)
                .endTimestamp(endTime)
                .build();

        metricsEventPublisher.publishMetricEvent(PartitionOffsetLagMetricEvent.from(partition.getToken(), startTime));

        return partition;
    }

    public List<Partition> getPartitions(List<PartitionState> partitionStates) {
        Map<String, Timestamp> preloadedOffsets = partitionOffsetProvider.getOffsets(
                partitionStates.stream().map(PartitionState::getToken).collect(Collectors.toList()));
        return partitionStates.stream()
                .map(ps -> Partition.builder()
                        .token(ps.getToken())
                        .startTimestamp(resolveStartTime(ps, preloadedOffsets.get(ps.getToken())))
                        .endTimestamp(ps.getEndTimestamp())
                        .parentTokens(ps.getParents())
                        .build())
                .collect(Collectors.toList());
    }

    public Partition getPartition(PartitionState partitionState) {
        return Partition.builder()
                .token(partitionState.getToken())
                .startTimestamp(getOffset(partitionState))
                .endTimestamp(partitionState.getEndTimestamp())
                .parentTokens(partitionState.getParents())
                .build();
    }

    private Timestamp getOffset(PartitionState partitionState) {
        final Timestamp offset = partitionOffsetProvider.getOffset(partitionState);
        Timestamp startTime = resolveStartTime(partitionState, offset);
        metricsEventPublisher.publishMetricEvent(PartitionOffsetLagMetricEvent.from(partitionState.getToken(), startTime));
        return startTime;
    }

    private Timestamp resolveStartTime(PartitionState partitionState, Timestamp offset) {
        if (offset != null) {
            if (offset.toSqlTimestamp().before(partitionState.getStartTimestamp().toSqlTimestamp())) {
                Map<String, String> offsetMap = partitionOffsetProvider.getOffsetMap(partitionState);
                LOGGER.warn("Incorrect offset, start time will be taken for partition {}, offsetMap {}", partitionState.getToken(), offsetMap);
                return partitionState.getStartTimestamp();
            }
            LOGGER.info("Found previous offset {}", Map.of(partitionState.getToken(), offset.toString()));
            return offset;
        }
        LOGGER.info("Previous offset not found, start time will be taken {}",
                Map.of(partitionState.getToken(), partitionState.getStartTimestamp()));
        return partitionState.getStartTimestamp();
    }
}
