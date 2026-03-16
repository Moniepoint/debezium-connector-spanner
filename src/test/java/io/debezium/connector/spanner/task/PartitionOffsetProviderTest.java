/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;

/**
 * Tests for interrupt handling in PartitionOffsetProvider.
 *
 * Verifies that when the calling thread is interrupted while waiting for the offset storage
 * response (future.get()), the provider:
 *   1. Returns null from retrieveOffsetMap(), causing getOffset() to fall back to startTimestamp.
 *   2. Re-sets the interrupt flag on the calling thread so the interrupt is not silently swallowed.
 *
 * These invariants are the foundation for the cascade-prevention fix in
 * TakePartitionForStreamingOperation: the re-set interrupt flag is what isInterrupted() detects
 * to break the loop early.
 *
 * Note: MetricsEventPublisher is a concrete class; we instantiate it directly (no-op when no
 * subscribers are registered) rather than using Mockito.
 */
class PartitionOffsetProviderTest {

    @AfterEach
    void clearInterruptFlag() {
        Thread.interrupted();
    }

    /**
     * Pre-interrupting the calling thread before getOffset() causes future.get() to throw
     * InterruptedException immediately (FutureTask checks the flag at entry of awaitDone()).
     * The provider must fall back to startTimestamp and preserve the interrupt flag.
     */
    @Test
    void whenCallerPreInterrupted_getOffsetReturnsFallbackTimestampAndRestoresFlag() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(1_000_000L);
        PartitionState partitionState = buildPartitionState("token-1", startTimestamp);

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, new MetricsEventPublisher());

        // Simulate stopProcessing() having already interrupted this thread before getOffset() runs
        Thread.currentThread().interrupt();

        Timestamp result = provider.getOffset(partitionState);

        // No saved offset found (retrieval aborted) → must fall back to startTimestamp
        assertThat(result)
                .as("must fall back to startTimestamp when retrieval is interrupted")
                .isEqualTo(startTimestamp);

        // The interrupt flag must be preserved — TakePartitionForStreamingOperation's
        // isInterrupted() check relies on this to break the loop
        assertThat(Thread.currentThread().isInterrupted())
                .as("interrupt flag must be re-set after catching InterruptedException")
                .isTrue();
    }

    /**
     * Simulates the actual production scenario: the calling thread blocks inside future.get()
     * while the executor thread is waiting for the offset storage response, then the caller is
     * interrupted mid-wait (as stopProcessing() does via thread.interrupt()).
     *
     * The provider must unblock, fall back to startTimestamp, and re-set the interrupt flag.
     */
    @Test
    void whenCallerInterruptedWhileWaitingForFuture_getOffsetReturnsFallbackAndRestoresFlag()
            throws InterruptedException {

        CountDownLatch readerStarted = new CountDownLatch(1);
        CountDownLatch releaseReader = new CountDownLatch(1);

        OffsetStorageReader reader = mock(OffsetStorageReader.class);

        // Block the executor thread so the caller is definitely inside future.get() when interrupted
        when(reader.offset(any())).thenAnswer(inv -> {
            readerStarted.countDown(); // signal: caller is now blocked in future.get()
            releaseReader.await(); // hold until test releases
            return null;
        });

        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(2_000_000L);
        PartitionState partitionState = buildPartitionState("token-2", startTimestamp);
        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, new MetricsEventPublisher());

        AtomicReference<Timestamp> result = new AtomicReference<>();
        AtomicBoolean interruptFlagAfterCall = new AtomicBoolean(false);

        Thread callerThread = new Thread(() -> {
            result.set(provider.getOffset(partitionState));
            interruptFlagAfterCall.set(Thread.currentThread().isInterrupted());
        }, "test-caller");
        callerThread.start();

        // Wait until the executor thread is blocked inside reader.offset(),
        // meaning the caller is now blocked inside future.get()
        readerStarted.await();
        callerThread.interrupt(); // mirrors stopProcessing() → thread.interrupt()
        callerThread.join(3_000);

        releaseReader.countDown(); // unblock the executor thread (cleanup)

        assertThat(callerThread.getState())
                .as("caller thread must have finished")
                .isEqualTo(Thread.State.TERMINATED);
        assertThat(result.get())
                .as("must fall back to startTimestamp when retrieval is interrupted mid-wait")
                .isEqualTo(startTimestamp);
        assertThat(interruptFlagAfterCall.get())
                .as("interrupt flag must be preserved after getOffset() returns")
                .isTrue();
    }

    /**
     * Verifies that a successful (non-interrupted) offset retrieval does NOT set the interrupt flag,
     * establishing the baseline for the interrupt-flag assertion in the other tests.
     */
    @Test
    void whenOffsetRetrievedSuccessfully_interruptFlagIsNotSet() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        // Return null from reader → "no saved offset" path → falls back to startTimestamp
        when(reader.offset(any())).thenReturn(null);

        Timestamp startTimestamp = Timestamp.ofTimeMicroseconds(3_000_000L);
        PartitionState partitionState = buildPartitionState("token-3", startTimestamp);
        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, new MetricsEventPublisher());

        Timestamp result = provider.getOffset(partitionState);

        assertThat(result).isEqualTo(startTimestamp);
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    /**
     * Reproduces the scenario observed in 16032026-debezium.log:
     * the task thread is interrupted during shutdown while getOffsets() is blocked inside
     * OffsetStorageReaderImpl.offsets(). The Kafka Connect framework wraps the
     * InterruptedException in a ConnectException. Without the fix this propagated uncaught,
     * generating a noisy ERROR log and a "follow-up failure exception" entry.
     *
     * After the fix getOffsets() must:
     *   1. Return an empty map (callers fall back to startTimestamp).
     *   2. Re-set the interrupt flag so the task shutdown sequence sees it.
     */
    @Test
    void whenOffsetStorageReaderThrowsConnectExceptionWrappingInterrupt_getOffsetsReturnsEmptyMapAndRestoresFlag() {
        OffsetStorageReader reader = mock(OffsetStorageReader.class);
        when(reader.offsets(any())).thenThrow(new ConnectException(new InterruptedException("simulated shutdown interrupt")));

        PartitionOffsetProvider provider = new PartitionOffsetProvider(reader, new MetricsEventPublisher());

        Map<String, com.google.cloud.Timestamp> result = provider.getOffsets(List.of("token-A", "token-B"));

        assertThat(result).as("must return empty map when interrupted").isEmpty();
        assertThat(Thread.currentThread().isInterrupted()).as("interrupt flag must be re-set").isTrue();
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
