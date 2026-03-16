/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

/**
 * Regression tests for the thundering-herd fix in PartitionFactory.
 *
 * Problem: when many partitions restart simultaneously (e.g. after a rebalance),
 * the old per-partition path called partitionOffsetProvider.getOffset() once per partition.
 * Under Confluent Cloud startup load, retrieveOffsetMap() timed out (5-second hard cutoff),
 * returned null, and getOffset() fell back to partitionState.getStartTimestamp() — potentially
 * hours old. Partitions then re-streamed stale Spanner history instead of resuming from the
 * last committed Kafka offset, explaining the "low production rate" observed in logs.
 *
 * Fix: TakePartitionForStreamingOperation now calls PartitionFactory.getPartitions(List) before
 * the submission loop. That method delegates to partitionOffsetProvider.getOffsets(Collection)
 * — a single batch call with no timeout — so all partitions get the correct recent offset
 * regardless of load.
 *
 * These tests verify:
 *   1. Old per-partition path (getPartition) returns stale startTimestamp when getOffset times out.
 *   2. New batch path (getPartitions) returns the actual recent Kafka offset.
 *   3. With N partitions the batch path makes exactly one getOffsets() call, not N getOffset() calls.
 */
class PartitionFactoryThunderingHerdTest {

    /**
     * Core behavioral difference: single partition.
     *
     * getOffset() simulates a timeout fallback (returns startTimestamp, the stale "hours old" value).
     * getOffsets() simulates a successful batch read (returns the actual recent offset).
     *
     * getPartition() → stale startTimestamp (wrong)
     * getPartitions() → recent offset (correct)
     */
    @Test
    void singlePartition_getPartitionsReturnsCorrectOffsetWhereGetPartitionWouldReturnFallback() {
        Timestamp staleStart = Timestamp.ofTimeMicroseconds(1_000_000L); // "hours old" startTimestamp
        Timestamp recentOffset = Timestamp.ofTimeMicroseconds(5_000_000L); // actual saved Kafka offset

        PartitionOffsetProvider provider = mock(PartitionOffsetProvider.class);
        // Simulates timeout: getOffset falls back to partitionState.startTimestamp
        when(provider.getOffset(any())).thenReturn(staleStart);
        // Simulates successful batch read: no timeout, returns real recent offset
        when(provider.getOffsets(any())).thenReturn(Map.of("token-1", recentOffset));

        PartitionFactory factory = new PartitionFactory(provider, new MetricsEventPublisher());
        PartitionState ps = buildPartitionState("token-1", staleStart);

        // Old per-partition path: falls back to stale startTimestamp
        Partition oldPathResult = factory.getPartition(ps);
        assertThat(oldPathResult.getStartTimestamp())
                .as("old per-partition path returns stale startTimestamp when getOffset() times out")
                .isEqualTo(staleStart);

        // New bulk path: returns the actual recent Kafka offset
        List<Partition> newPathResult = factory.getPartitions(List.of(ps));
        assertThat(newPathResult.get(0).getStartTimestamp())
                .as("new bulk path returns the actual recent offset even under load")
                .isEqualTo(recentOffset);
    }

    /**
     * N partitions: the batch path makes exactly ONE getOffsets() call.
     *
     * The old path made N getOffset() calls — N independent 5-second timeouts in parallel
     * under load. The new path makes a single getOffsets() call for all tokens at once,
     * so there is no per-partition timeout exposure.
     */
    @Test
    void nPartitions_getPartitionsMakesExactlyOneBatchCall() {
        int n = 50;
        Timestamp staleStart = Timestamp.ofTimeMicroseconds(1_000_000L);
        Timestamp recentOffset = Timestamp.ofTimeMicroseconds(5_000_000L);

        PartitionOffsetProvider provider = mock(PartitionOffsetProvider.class);
        Map<String, Timestamp> batchResult = IntStream.range(0, n)
                .boxed()
                .collect(Collectors.toMap(i -> "token-" + i, i -> recentOffset));
        when(provider.getOffsets(any())).thenReturn(batchResult);

        PartitionFactory factory = new PartitionFactory(provider, new MetricsEventPublisher());
        List<PartitionState> partitions = IntStream.range(0, n)
                .mapToObj(i -> buildPartitionState("token-" + i, staleStart))
                .collect(Collectors.toList());

        List<Partition> result = factory.getPartitions(partitions);

        // Exactly ONE batch call — not N per-partition calls
        verify(provider, never()).getOffset(any());
        verify(provider).getOffsets(any());

        // All partitions receive the correct recent offset
        assertThat(result).hasSize(n);
        assertThat(result)
                .extracting(Partition::getStartTimestamp)
                .as("all partitions must use the recent offset, not the stale startTimestamp")
                .containsOnly(recentOffset);
    }

    /**
     * When the batch reader returns no result for a token (e.g. truly new partition with no
     * prior Kafka offset), the factory correctly falls back to startTimestamp — same behaviour
     * as the old path on a fresh partition.
     */
    @Test
    void whenBatchReaderReturnsNoOffsetForToken_fallsBackToStartTimestamp() {
        Timestamp staleStart = Timestamp.ofTimeMicroseconds(1_000_000L);

        PartitionOffsetProvider provider = mock(PartitionOffsetProvider.class);
        // Empty map: no saved offset for this token
        when(provider.getOffsets(any())).thenReturn(Map.of());

        PartitionFactory factory = new PartitionFactory(provider, new MetricsEventPublisher());
        PartitionState ps = buildPartitionState("token-new", staleStart);

        List<Partition> result = factory.getPartitions(List.of(ps));

        assertThat(result.get(0).getStartTimestamp())
                .as("new partition with no saved offset must fall back to startTimestamp")
                .isEqualTo(staleStart);
    }

    // ---- helpers ----

    private PartitionState buildPartitionState(String token, Timestamp startTimestamp) {
        return PartitionState.builder()
                .token(token)
                .state(PartitionStateEnum.READY_FOR_STREAMING)
                .startTimestamp(startTimestamp)
                .parents(Set.of("Parent0"))
                .assigneeTaskUid("task-0")
                .build();
    }
}
