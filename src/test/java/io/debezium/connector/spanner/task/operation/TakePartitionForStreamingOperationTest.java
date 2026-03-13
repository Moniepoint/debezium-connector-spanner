/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.task.PartitionFactory;
import io.debezium.connector.spanner.task.TaskSyncContext;

/**
 * Tests for TakePartitionForStreamingOperation.
 *
 * Since PartitionFactory.getPartitions() bulk-loads offsets before the submission loop,
 * interrupt scenarios are tested by setting the interrupt flag either during the bulk
 * load (getPartitions override) or during changeStream.submitPartition().
 *
 * Note: PartitionFactory is a concrete class that cannot be mocked on Java 25 with Mockito inline
 * mocks. We use anonymous subclasses instead.
 */
class TakePartitionForStreamingOperationTest {

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    /**
     * When the thread is interrupted during the bulk offset load (getPartitions), the
     * loop's isInterrupted() check fires on the first iteration and nothing is submitted.
     */
    @Test
    void whenInterruptFlagSetDuringBulkLoad_loopNeverSubmitsAnyPartition() {
        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public List<Partition> getPartitions(List<PartitionState> psList) {
                // Simulates retrieveOffsetMap() catching InterruptedException and re-setting the flag
                Thread.currentThread().interrupt();
                return psList.stream().map(ps -> buildPartition(ps.getToken())).collect(Collectors.toList());
            }
        };

        ChangeStream changeStream = mock(ChangeStream.class);

        TaskSyncContext context = buildContextWithReadyPartitions(3);
        new TakePartitionForStreamingOperation(changeStream, partitionFactory).doOperation(context);

        verify(changeStream, never()).submitPartition(any());
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    /**
     * When the thread is already interrupted before the operation starts, the loop's
     * first isInterrupted() check fires and all partitions are skipped entirely.
     */
    @Test
    void whenThreadAlreadyInterruptedBeforeLoop_allPartitionsSkipped() {
        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public List<Partition> getPartitions(List<PartitionState> psList) {
                return psList.stream().map(ps -> buildPartition(ps.getToken())).collect(Collectors.toList());
            }
        };

        ChangeStream changeStream = mock(ChangeStream.class);

        TaskSyncContext context = buildContextWithReadyPartitions(3);
        Thread.currentThread().interrupt();
        new TakePartitionForStreamingOperation(changeStream, partitionFactory).doOperation(context);

        verify(changeStream, never()).submitPartition(any());
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    /**
     * Positive path: without any interrupt all READY_FOR_STREAMING partitions are processed
     * and transition to SCHEDULED state.
     */
    @Test
    void withoutInterrupt_allReadyForStreamingPartitionsAreScheduled() {
        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public List<Partition> getPartitions(List<PartitionState> psList) {
                return psList.stream().map(ps -> buildPartition(ps.getToken())).collect(Collectors.toList());
            }
        };

        ChangeStream changeStream = mock(ChangeStream.class);
        when(changeStream.submitPartition(any())).thenReturn(true);

        TaskSyncContext context = buildContextWithReadyPartitions(3);
        TaskSyncContext result = new TakePartitionForStreamingOperation(changeStream, partitionFactory)
                .doOperation(context);

        verify(changeStream, times(3)).submitPartition(any());

        long scheduledCount = result.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getState() == PartitionStateEnum.SCHEDULED)
                .count();
        assertThat(scheduledCount).isEqualTo(3);
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    /**
     * When the interrupt flag is set during the first submitPartition() call, the loop breaks
     * on the second iteration's isInterrupted() check. Remaining partitions stay READY_FOR_STREAMING.
     */
    @Test
    void whenInterruptMidBatch_unprocessedPartitionsStayReadyForStreaming() {
        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public List<Partition> getPartitions(List<PartitionState> psList) {
                return psList.stream().map(ps -> buildPartition(ps.getToken())).collect(Collectors.toList());
            }
        };

        AtomicInteger submitCalls = new AtomicInteger();
        ChangeStream changeStream = mock(ChangeStream.class);
        when(changeStream.submitPartition(any())).thenAnswer(inv -> {
            if (submitCalls.incrementAndGet() == 1) {
                Thread.currentThread().interrupt();
            }
            return false;
        });

        TaskSyncContext context = buildContextWithReadyPartitions(3);
        TaskSyncContext result = new TakePartitionForStreamingOperation(changeStream, partitionFactory)
                .doOperation(context);

        // Only first partition was attempted before loop broke
        assertThat(submitCalls.get()).isEqualTo(1);

        // All 3 must still be READY_FOR_STREAMING — none promoted to SCHEDULED
        long readyCount = result.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getState() == PartitionStateEnum.READY_FOR_STREAMING)
                .count();
        assertThat(readyCount)
                .as("unprocessed partitions must remain READY_FOR_STREAMING for the new task instance")
                .isEqualTo(3);
    }

    /**
     * When the ChangeStream is not yet running, submitPartition() returns false.
     * All partitions must stay READY_FOR_STREAMING for the next retry and no sync event published.
     */
    @Test
    void whenChangeStreamNotRunning_allPartitionsStayReadyForStreamingForRetry() {
        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public List<Partition> getPartitions(List<PartitionState> psList) {
                return psList.stream().map(ps -> buildPartition(ps.getToken())).collect(Collectors.toList());
            }
        };

        ChangeStream changeStream = mock(ChangeStream.class);
        when(changeStream.submitPartition(any())).thenReturn(false);

        TaskSyncContext context = buildContextWithReadyPartitions(3);
        TakePartitionForStreamingOperation op = new TakePartitionForStreamingOperation(changeStream, partitionFactory);
        TaskSyncContext result = op.doOperation(context);

        verify(changeStream, times(3)).submitPartition(any());

        long readyCount = result.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getState() == PartitionStateEnum.READY_FOR_STREAMING)
                .count();
        assertThat(readyCount)
                .as("partitions must stay READY_FOR_STREAMING when stream is not running")
                .isEqualTo(3);

        assertThat(op.isRequiredPublishSyncEvent())
                .as("no sync event should be published when no partition was scheduled")
                .isFalse();
    }

    // ---- helpers ----

    private Partition buildPartition(String token) {
        return Partition.builder()
                .token(token)
                .parentTokens(Set.of("Parent0"))
                .startTimestamp(Timestamp.now())
                .build();
    }

    private TaskSyncContext buildContextWithReadyPartitions(int count) {
        List<PartitionState> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(PartitionState.builder()
                    .token("token-" + i)
                    .state(PartitionStateEnum.READY_FOR_STREAMING)
                    .startTimestamp(Timestamp.now())
                    .parents(Set.of("Parent0"))
                    .assigneeTaskUid("task-0")
                    .build());
        }
        return TaskSyncContext.builder()
                .taskUid("task-0")
                .rebalanceState(RebalanceState.NEW_EPOCH_STARTED)
                .currentTaskState(TaskState.builder()
                        .taskUid("task-0")
                        .partitions(partitions)
                        .sharedPartitions(List.of())
                        .build())
                .taskStates(Map.of())
                .build();
    }
}
