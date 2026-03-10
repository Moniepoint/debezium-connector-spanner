/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
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
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.exception.SpannerConnectorException;

/**
 * Verifies the graceful-shutdown fix in RebalancingEventListener:
 *
 *  - When the thread is interrupted (normal task stop or worker rebalance),
 *    errorHandler must NOT be called — it is not a task failure.
 *  - When the thread exits due to an unexpected exception (real connectivity loss),
 *    errorHandler MUST be called exactly once.
 *
 * Note: org.apache.kafka.clients.consumer.Consumer extends java.io.Closeable which lives in the
 * java.base sealed module — Byte Buddy on Java 25 cannot instrument it.
 * We use a hand-written StubConsumer base class (same pattern as TakePartitionForStreamingOperationTest)
 * instead of Mockito mocks.
 */
class RebalancingEventListenerTest {

    static {
        // Byte Buddy officially supports up to Java 22; Java 25 requires experimental mode.
        // Set before the first mock() call so Byte Buddy picks it up when creating class files.
        System.setProperty("net.bytebuddy.experimental", "true");
    }

    // -------------------------------------------------------------------------
    // 1. Kafka InterruptException during poll → thread exits via interrupt path
    // → errorHandler must NOT be called
    // -------------------------------------------------------------------------
    @Test
    void interruptExceptionDuringPoll_errorHandlerIsNotCalled() throws InterruptedException {
        StubConsumer consumer = new StubConsumer() {
            @Override
            public ConsumerRecords<Object, Object> poll(Duration timeout) {
                throw new InterruptException("poll interrupted");
            }
        };

        CapturingErrorHandler errorHandler = new CapturingErrorHandler();

        buildListener(consumer, errorHandler).listen(meta -> {
        });

        Thread.sleep(300);

        assertThat(errorHandler.callCount()).as("errorHandler must not be called on interrupt").isEqualTo(0);
    }

    // -------------------------------------------------------------------------
    // 2. External shutdown() interrupt → errorHandler must NOT be called
    // -------------------------------------------------------------------------
    @Test
    void externalShutdown_errorHandlerIsNotCalled() throws InterruptedException {
        CountDownLatch pollingStarted = new CountDownLatch(1);

        StubConsumer consumer = new StubConsumer() {
            @Override
            public ConsumerRecords<Object, Object> poll(Duration timeout) {
                pollingStarted.countDown();
                try {
                    Thread.sleep(10_000);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InterruptException(e);
                }
                return ConsumerRecords.empty();
            }
        };

        CapturingErrorHandler errorHandler = new CapturingErrorHandler();

        RebalancingEventListener listener = buildListener(consumer, errorHandler);
        listener.listen(meta -> {
        });
        pollingStarted.await(2, TimeUnit.SECONDS);

        listener.shutdown();

        assertThat(errorHandler.callCount()).as("errorHandler must not be called on external shutdown").isEqualTo(0);
    }

    // -------------------------------------------------------------------------
    // 3. Unexpected exception (not interrupt) → real failure → errorHandler IS called
    // -------------------------------------------------------------------------
    @Test
    void unexpectedExceptionDuringPoll_errorHandlerIsCalledOnce() throws InterruptedException {
        StubConsumer consumer = new StubConsumer() {
            @Override
            public ConsumerRecords<Object, Object> poll(Duration timeout) {
                throw new KafkaException("unexpected network failure");
            }
        };

        CapturingErrorHandler errorHandler = new CapturingErrorHandler();

        buildListener(consumer, errorHandler).listen(meta -> {
        });

        Thread.sleep(300);

        assertThat(errorHandler.callCount()).as("errorHandler must be called exactly once on real failure").isEqualTo(1);
        assertThat(errorHandler.lastException()).isInstanceOf(SpannerConnectorException.class);
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

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

        synchronized RuntimeException lastException() {
            return exceptions.isEmpty() ? null : exceptions.get(exceptions.size() - 1);
        }
    }

    @SuppressWarnings("unchecked")
    private RebalancingEventListener buildListener(
                                                   org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                                   Consumer<RuntimeException> errorHandler) {
        SpannerConnectorTask task = mock(SpannerConnectorTask.class);
        when(task.getTaskUid()).thenReturn("test-task-uid");

        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.rebalancingPollDuration()).thenReturn(50);
        when(config.rebalancingCommitOffsetsTimeout()).thenReturn(1000);
        when(config.rebalancingCommitOffsetsInterval()).thenReturn(30_000);

        RebalancingConsumerFactory<?, ?> factory = mock(RebalancingConsumerFactory.class);
        when(factory.getConfig()).thenReturn(config);
        doReturn(consumer).when(factory).createSubscribeConsumer(any(), any(), any());

        return new RebalancingEventListener(
                task, "test-group", "test-topic",
                Duration.ofMillis(500), factory, errorHandler);
    }

    // -------------------------------------------------------------------------
    // StubConsumer — implements the full Kafka 4.x Consumer interface with no-ops.
    // Override poll(Duration) per test case to inject the desired behaviour.
    // Using a hand-written stub because Byte Buddy (Mockito inline mocks) cannot
    // instrument java.io.Closeable on Java 25.
    // -------------------------------------------------------------------------
    private abstract static class StubConsumer
            implements org.apache.kafka.clients.consumer.Consumer<Object, Object> {

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
        public abstract ConsumerRecords<Object, Object> poll(Duration timeout);

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
        public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
            return Map.of();
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
            return Map.of();
        }

        @Override
        public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
            return Map.of();
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
            return Map.of();
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
            return Map.of();
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
