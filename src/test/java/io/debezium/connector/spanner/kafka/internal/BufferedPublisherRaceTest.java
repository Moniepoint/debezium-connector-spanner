/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.RebalanceState;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;

/**
 * Deterministic regression test for the race condition in BufferedPublisher.
 *
 * Root cause: publishBuffered() was not synchronized. The background thread could capture a
 * buffered item via getAndSet(null) and then lose the CPU before calling onPublish — while the
 * main thread entered its synchronized(this) block and published a newer immediate item first.
 * Result: stale buffered item published AFTER the fresh immediate item (ordering inversion).
 *
 * Fix: publishBuffered() is now synchronized on the same monitor as buffer()'s immediate path,
 * making the two publish paths mutually exclusive.
 *
 * The test forces the race deterministically:
 *   1. A non-immediate item (1) is buffered; the background thread picks it up and calls onPublish(1).
 *   2. onPublish blocks on a latch, holding the synchronized lock.
 *   3. An immediate item (10) is published from a second thread — it tries synchronized(this)
 *      and blocks on the lock.
 *   4. The latch is released; background finishes onPublish(1) and releases the lock.
 *   5. The immediate thread unblocks and publishes 10.
 *   6. Result must be [1, 10] — the buffered item before the immediate item.
 *
 * Without the fix (publishBuffered() unsynchronized): step 3 would not block, immediate(10)
 * would be published before buffered(1) completes, producing [10, 1].
 */
class BufferedPublisherRaceTest {

    @Test
    void synchronizedPublishBuffered_immediateItemCannotOvertakeInFlightBufferedPublish()
            throws InterruptedException {

        List<Integer> result = new CopyOnWriteArrayList<>();
        CountDownLatch bufferedOnPublishEntered = new CountDownLatch(1);
        CountDownLatch releaseBufferedOnPublish = new CountDownLatch(1);
        AtomicBoolean firstOnPublish = new AtomicBoolean(true);

        Consumer<Integer> onPublish = v -> {
            if (firstOnPublish.compareAndSet(true, false)) {
                // Signal: background thread is now inside onPublish (holding the synchronized lock)
                bufferedOnPublishEntered.countDown();
                try {
                    releaseBufferedOnPublish.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            result.add(v);
        };

        MetricsEventPublisher metrics = mock(MetricsEventPublisher.class);
        TaskSyncContextHolder holder = new TaskSyncContextHolder(metrics);
        holder.init(TaskSyncContext.builder()
                .taskUid("t1")
                .rebalanceState(RebalanceState.NEW_EPOCH_STARTED)
                .build());

        // timeout=5ms so the background thread drains quickly; publishImmediately for multiples of 10
        BufferedPublisher<Integer> pub = new BufferedPublisher<>("t1", "pub", holder, 5,
                p -> p % 10 == 0, onPublish);
        pub.start();

        pub.buffer(1); // non-immediate → stored in AtomicRef; background will pick it up

        // Wait until background is inside onPublish(1) — it now holds the synchronized lock
        bufferedOnPublishEntered.await();

        // Attempt to publish an immediate item from a separate thread (mirrors the connector's main thread)
        Thread immediateThread = new Thread(() -> pub.buffer(10), "immediate-publisher");
        immediateThread.start();

        // Allow time for immediateThread to reach synchronized(this) in buffer() and block on the lock
        immediateThread.join(50);

        // With the fix: immediateThread is BLOCKED — it cannot bypass the in-flight buffered publish.
        // Without the fix: immediateThread would have already finished (no lock to contend), and
        // result would already contain 10, breaking the ordering invariant.
        assertThat(immediateThread.getState())
                .as("immediate item must not bypass the in-flight buffered publish")
                .isIn(Thread.State.BLOCKED, Thread.State.WAITING, Thread.State.TIMED_WAITING);

        // Release the background thread — it finishes onPublish(1) and releases the lock
        releaseBufferedOnPublish.countDown();
        immediateThread.join(1000); // immediate thread unblocks, publishes 10
        pub.close();

        // Ordering invariant: buffered item 1 must appear before immediate item 10
        assertThat(result)
                .as("buffered item must be published before the immediate item, not overtaken")
                .containsExactly(1, 10);
    }
}
