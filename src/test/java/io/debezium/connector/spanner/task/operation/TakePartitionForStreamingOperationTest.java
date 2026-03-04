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
 * Tests for the interrupt-cascade fix in TakePartitionForStreamingOperation.
 *
 * Scenario being tested:
 *   During AFTER_COMMIT_NO_PARENT_WAIT epoch transition, stopProcessing() interrupts the
 *   TaskStateChangeEventProcessor thread while it is inside future.get() in retrieveOffsetMap().
 *   That catch block calls Thread.currentThread().interrupt(), setting the flag on the caller.
 *   Without the fix (forEach), every subsequent partition would fail instantly and log an ERROR.
 *   With the fix (for-loop + isInterrupted() check), the loop breaks after the first interrupted
 *   partition and the remaining ones are left untouched for the new task instance to handle.
 *
 * Note: PartitionFactory is a concrete class that cannot be mocked on Java 25 with Mockito inline
 * mocks. We use anonymous subclasses with AtomicInteger call counters instead.
 */
class TakePartitionForStreamingOperationTest {

    @AfterEach
    void clearInterruptFlag() {
        // Ensure interrupt flag never leaks between tests
        Thread.interrupted();
    }

    /**
     * Core regression test for the cascade fix.
     *
     * The first partition's processing sets the interrupt flag (as retrieveOffsetMap() does when
     * future.get() is interrupted). The loop must break immediately on the second iteration check,
     * so partitionFactory.getPartition() is called exactly once — not N times.
     */
    @Test
    void whenInterruptFlagSetDuringFirstPartition_loopBreaksAndRemainingPartitionsAreSkipped() {
        AtomicInteger getPartitionCalls = new AtomicInteger(0);
        Partition dummyPartition = buildPartition("token-0");

        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public Partition getPartition(PartitionState ps) {
                getPartitionCalls.incrementAndGet();
                // Simulates what retrieveOffsetMap() does after catching InterruptedException:
                // sets the thread interrupt flag then returns (getOffset falls back to startTimestamp).
                Thread.currentThread().interrupt();
                return dummyPartition;
            }
        };

        ChangeStream changeStream = mock(ChangeStream.class);
        when(changeStream.submitPartition(any())).thenReturn(false);

        TaskSyncContext context = buildContextWithReadyPartitions(3);
        new TakePartitionForStreamingOperation(changeStream, partitionFactory).doOperation(context);

        // Loop must have broken after the first partition
        assertThat(getPartitionCalls.get())
                .as("getPartition() must be called only once before the loop breaks")
                .isEqualTo(1);
        verify(changeStream, times(1)).submitPartition(any());

        // Interrupt flag must still be set — not swallowed
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    /**
     * Verifies the other edge of the fix: when the thread is already interrupted *before*
     * the loop starts (possible if a prior operation already set the flag), the isInterrupted()
     * check fires on the very first iteration and nothing is submitted at all.
     */
    @Test
    void whenThreadAlreadyInterruptedBeforeLoop_allPartitionsSkipped() {
        AtomicInteger getPartitionCalls = new AtomicInteger(0);

        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public Partition getPartition(PartitionState ps) {
                getPartitionCalls.incrementAndGet();
                return buildPartition(ps.getToken());
            }
        };

        ChangeStream changeStream = mock(ChangeStream.class);

        TaskSyncContext context = buildContextWithReadyPartitions(3);

        // Pre-interrupt: simulates the state AFTER a prior retrieveOffsetMap() call set the flag
        Thread.currentThread().interrupt();
        new TakePartitionForStreamingOperation(changeStream, partitionFactory).doOperation(context);

        assertThat(getPartitionCalls.get())
                .as("no partition should be processed when thread is already interrupted")
                .isEqualTo(0);
        verify(changeStream, never()).submitPartition(any());
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    /**
     * Positive path: without any interrupt all READY_FOR_STREAMING partitions are processed
     * and transition to SCHEDULED state.
     */
    @Test
    void withoutInterrupt_allReadyForStreamingPartitionsAreScheduled() {
        AtomicInteger getPartitionCalls = new AtomicInteger(0);

        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public Partition getPartition(PartitionState ps) {
                getPartitionCalls.incrementAndGet();
                return buildPartition(ps.getToken());
            }
        };

        ChangeStream changeStream = mock(ChangeStream.class);
        when(changeStream.submitPartition(any())).thenReturn(true);

        TaskSyncContext context = buildContextWithReadyPartitions(3);
        TaskSyncContext result = new TakePartitionForStreamingOperation(changeStream, partitionFactory)
                .doOperation(context);

        assertThat(getPartitionCalls.get())
                .as("all 3 partitions must be processed without interrupt")
                .isEqualTo(3);
        verify(changeStream, times(3)).submitPartition(any());

        long scheduledCount = result.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getState() == PartitionStateEnum.SCHEDULED)
                .count();
        assertThat(scheduledCount).isEqualTo(3);
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    /**
     * Verifies that partitions which were not reached by the loop (because the interrupt fired
     * mid-batch) remain in READY_FOR_STREAMING state, so the new task instance can pick them up.
     */
    @Test
    void whenInterruptMidBatch_unprocessedPartitionsStayReadyForStreaming() {
        AtomicInteger getPartitionCalls = new AtomicInteger(0);

        PartitionFactory partitionFactory = new PartitionFactory(null, new MetricsEventPublisher()) {
            @Override
            public Partition getPartition(PartitionState ps) {
                int call = getPartitionCalls.incrementAndGet();
                if (call == 1) {
                    Thread.currentThread().interrupt(); // first partition triggers the cascade point
                }
                return buildPartition(ps.getToken());
            }
        };

        ChangeStream changeStream = mock(ChangeStream.class);
        when(changeStream.submitPartition(any())).thenReturn(false);

        TaskSyncContext context = buildContextWithReadyPartitions(3);
        TaskSyncContext result = new TakePartitionForStreamingOperation(changeStream, partitionFactory)
                .doOperation(context);

        // Only first partition was attempted
        assertThat(getPartitionCalls.get()).isEqualTo(1);

        // All 3 must still be READY_FOR_STREAMING — none promoted to SCHEDULED
        long readyCount = result.getCurrentTaskState().getPartitions().stream()
                .filter(p -> p.getState() == PartitionStateEnum.READY_FOR_STREAMING)
                .count();
        assertThat(readyCount)
                .as("unprocessed partitions must remain READY_FOR_STREAMING for the new task instance")
                .isEqualTo(3);
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
