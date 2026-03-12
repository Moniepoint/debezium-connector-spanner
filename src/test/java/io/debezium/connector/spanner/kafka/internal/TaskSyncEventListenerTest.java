/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.SpannerConnectorConfig;

/**
 * Verifies the graceful-shutdown fix in TaskSyncEventListener:
 *
 *  - When the poll thread is interrupted (normal task stop or worker rebalance),
 *    errorHandler must NOT be called — it is not a task failure.
 *  - When the thread exits due to an unexpected exception (real connectivity loss),
 *    errorHandler MUST be called at least once.
 *
 * Note: org.apache.kafka.clients.consumer.Consumer is not mockable on Java 25 with Byte Buddy
 * (Closeable lives in the java.base sealed module).  We use a hand-written StubSyncConsumer
 * base class, same pattern as RebalancingEventListenerTest.
 */
class TaskSyncEventListenerTest {

    static {
        // Byte Buddy officially supports up to Java 22; Java 25 requires experimental mode.
        System.setProperty("net.bytebuddy.experimental", "true");
    }

    private static final String SYNC_TOPIC = "sync-topic";
    private static final TopicPartition SYNC_TP = new TopicPartition(SYNC_TOPIC, 0);

    // -----------------------------------------------------------------------
    // 1. Kafka InterruptException during poll → thread exits via interrupt path
    // → errorHandler must NOT be called
    // -----------------------------------------------------------------------
    @Test
    void interruptExceptionDuringPoll_errorHandlerIsNotCalled() throws Exception {
        CapturingErrorHandler errorHandler = new CapturingErrorHandler();
        CountDownLatch pollStarted = new CountDownLatch(1);

        StubSyncConsumer consumer = new StubSyncConsumer() {
            @Override
            public ConsumerRecords<String, byte[]> poll(Duration timeout) {
                pollStarted.countDown();
                throw new InterruptException("simulated interrupt during poll");
            }
        };

        TaskSyncEventListener listener = buildListener(consumer, errorHandler);
        listener.start();

        assertThat(pollStarted.await(5, TimeUnit.SECONDS))
                .as("poll() should have been called within 5 s").isTrue();
        Thread.sleep(150); // let the thread finish its finally block

        assertThat(errorHandler.callCount())
                .as("errorHandler must not fire when the thread is interrupted")
                .isZero();
    }

    // -----------------------------------------------------------------------
    // 2. External shutdown via listener.shutdown() → thread is interrupted
    // → errorHandler must NOT be called
    // -----------------------------------------------------------------------
    @Test
    void externalShutdown_errorHandlerIsNotCalled() throws Exception {
        CapturingErrorHandler errorHandler = new CapturingErrorHandler();
        CountDownLatch pollStarted = new CountDownLatch(1);

        StubSyncConsumer consumer = new StubSyncConsumer() {
            @Override
            public ConsumerRecords<String, byte[]> poll(Duration timeout) {
                pollStarted.countDown();
                return ConsumerRecords.empty();
            }
        };

        TaskSyncEventListener listener = buildListener(consumer, errorHandler);
        listener.start();

        assertThat(pollStarted.await(5, TimeUnit.SECONDS))
                .as("poll() should have been called within 5 s").isTrue();
        listener.shutdown(); // interrupts thread and blocks until terminated

        assertThat(errorHandler.callCount())
                .as("errorHandler must not fire on clean shutdown")
                .isZero();
    }

    // -----------------------------------------------------------------------
    // 3. Unexpected exception during poll → real connectivity loss
    // → errorHandler MUST be called
    // -----------------------------------------------------------------------
    @Test
    void unexpectedExceptionDuringPoll_errorHandlerIsCalled() throws Exception {
        CapturingErrorHandler errorHandler = new CapturingErrorHandler();
        CountDownLatch shutdownStarted = new CountDownLatch(1);

        StubSyncConsumer consumer = new StubSyncConsumer() {
            @Override
            public ConsumerRecords<String, byte[]> poll(Duration timeout) {
                throw new KafkaException("simulated real connectivity loss");
            }

            @Override
            public void unsubscribe() {
                shutdownStarted.countDown();
            }
        };

        TaskSyncEventListener listener = buildListener(consumer, errorHandler);
        listener.start();

        assertThat(shutdownStarted.await(5, TimeUnit.SECONDS))
                .as("shutdownConsumer should have been called within 5 s").isTrue();
        Thread.sleep(50);

        assertThat(errorHandler.callCount())
                .as("errorHandler must fire at least once on real poll failure")
                .isGreaterThanOrEqualTo(1);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private TaskSyncEventListener buildListener(
                                                StubSyncConsumer consumer,
                                                Consumer<RuntimeException> errorHandler) {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.syncPollDuration()).thenReturn(50);
        when(config.syncCommitOffsetsTimeout()).thenReturn(100);
        when(config.syncCommitOffsetsInterval()).thenReturn(Integer.MAX_VALUE);

        SyncEventConsumerFactory<String, byte[]> factory = mock(SyncEventConsumerFactory.class);
        when(factory.getConfig()).thenReturn(config);
        when(factory.isAutoCommitEnabled()).thenReturn(true);
        when(factory.createConsumer(any())).thenReturn(consumer);

        return new TaskSyncEventListener(
                "test-group", SYNC_TOPIC, factory, false, errorHandler);
    }

    /**
     * Hand-written error-handler stub — java.util.function.Consumer lives in the java.base
     * sealed module and cannot be instrumented by Byte Buddy on Java 25.
     */
    private static class CapturingErrorHandler implements Consumer<RuntimeException> {
        private final AtomicInteger count = new AtomicInteger(0);
        private final List<RuntimeException> exceptions = new ArrayList<>();

        @Override
        public synchronized void accept(RuntimeException e) {
            count.incrementAndGet();
            exceptions.add(e);
        }

        int callCount() {
            return count.get();
        }
    }

    /**
     * Hand-written stub for Consumer&lt;String, byte[]&gt; — Byte Buddy cannot instrument
     * Closeable (java.base) on Java 25.  Override poll() per test case.
     *
     * endOffsets / beginningOffsets return values that make TaskSyncEventListener
     * launch the polling thread (endOffset=5, beginOffset=0, startOffset=4).
     */
    private abstract static class StubSyncConsumer
            implements org.apache.kafka.clients.consumer.Consumer<String, byte[]> {

        @Override
        public Set<TopicPartition> assignment() {
            return Set.of();
        }

        @Override
        public Set<String> subscription() {
            return Set.of();
        }

        @Override
        public void subscribe(Collection<String> topics) {
        }

        @Override
        public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        }

        @Override
        public void assign(Collection<TopicPartition> partitions) {
        }

        @Override
        public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        }

        @Override
        public void subscribe(Pattern pattern) {
        }

        @Override
        public void subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback) {
        }

        @Override
        public void subscribe(SubscriptionPattern pattern) {
        }

        @Override
        public void unsubscribe() {
        }

        @Override
        public abstract ConsumerRecords<String, byte[]> poll(Duration timeout);

        @Override
        public void commitSync() {
        }

        @Override
        public void commitSync(Duration timeout) {
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        }

        @Override
        public void commitAsync() {
        }

        @Override
        public void commitAsync(OffsetCommitCallback callback) {
        }

        @Override
        public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        }

        @Override
        public void registerMetricForSubscription(KafkaMetric metric) {
        }

        @Override
        public void unregisterMetricFromSubscription(KafkaMetric metric) {
        }

        @Override
        public void seek(TopicPartition partition, long offset) {
        }

        @Override
        public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        }

        @Override
        public void seekToBeginning(Collection<TopicPartition> partitions) {
        }

        @Override
        public void seekToEnd(Collection<TopicPartition> partitions) {
        }

        @Override
        public long position(TopicPartition partition) {
            return 0;
        }

        @Override
        public long position(TopicPartition partition, Duration timeout) {
            return 0;
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
            return Map.of();
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
            return Map.of();
        }

        @Override
        public Uuid clientInstanceId(Duration timeout) {
            return null;
        }

        @Override
        public Map<MetricName, ? extends Metric> metrics() {
            return Map.of();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            return List.of();
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
            return List.of();
        }

        @Override
        public Map<String, List<PartitionInfo>> listTopics() {
            return Map.of();
        }

        @Override
        public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
            return Map.of();
        }

        @Override
        public Set<TopicPartition> paused() {
            return Set.of();
        }

        @Override
        public void pause(Collection<TopicPartition> partitions) {
        }

        @Override
        public void resume(Collection<TopicPartition> partitions) {
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
            return Map.of();
        }

        @Override
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                       Duration timeout) {
            return Map.of();
        }

        // Returns non-empty maps so that start() launches the polling thread
        // (endOffset=5 != startOffset=4, seekBackToPreviousEpoch=false → thread starts)
        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            return Map.of(SYNC_TP, 0L);
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
            return Map.of(SYNC_TP, 0L);
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            return Map.of(SYNC_TP, 5L);
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
            return Map.of(SYNC_TP, 5L);
        }

        @Override
        public OptionalLong currentLag(TopicPartition topicPartition) {
            return OptionalLong.empty();
        }

        @Override
        public ConsumerGroupMetadata groupMetadata() {
            return null;
        }

        @Override
        public void enforceRebalance() {
        }

        @Override
        public void enforceRebalance(String reason) {
        }

        @Override
        public void close() {
        }

        @Override
        public void close(Duration timeout) {
        }

        @Override
        public void close(CloseOptions options) {
        }

        @Override
        public void wakeup() {
        }
    }
}
