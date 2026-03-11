/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.function.BlockingConsumer;

class FinishingPartitionManagerTest {

    @Test
    void commitRecord() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void onPartitionFinishEvent() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void forceFinish() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);

        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);
        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.forceFinish("testToken");

        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void withoutRegistration() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");
    }

    @Test
    void multipleCommitFinishEventFirst() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaab");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaac");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitCommitFirst1() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaab");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaac");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitCommitFirst2() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaab");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaac");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitCommitOutOfOrder() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.newRecord("testToken");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaaa");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaac");

        finishingPartitionManager.commitRecord("testToken", "aaaaaaab");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");
        finishingPartitionManager.onPartitionFinishEvent("testToken");
        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitNoEvents() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitNoEventsWithWrongCommitCall() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.commitRecord("testToken", "recordUid3");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        finishingPartitionManager.onPartitionFinishEvent("testToken");

        // except call consumer.accept
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    /**
     * Regression test for FinishingPartitionTimeout recovery.
     *
     * When FinishPartitionWatchDog fires (a partition has been pending-finish for > 300s),
     * instead of calling processFailure() the connector now calls cancelPendingFinish() +
     * updateToReadyForStreaming(). This resets the stuck partition so it re-streams from
     * the last committed offset rather than killing the whole connector.
     *
     * This test verifies that after cancelPendingFinish():
     * 1. The partition is removed from getPendingFinishPartitions().
     * 2. forceFinish (consumer.accept) is NOT called.
     * 3. The partition can be re-registered for a fresh streaming cycle.
     */
    @Test
    void cancelPendingFinish_removesPartitionSoItCanBeReRegistered() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        // Simulate: partition emitted records, finish event arrived, but last record not committed
        finishingPartitionManager.registerPartition("testToken");
        finishingPartitionManager.newRecord("testToken"); // lastEmitted = aaaaaaaa
        finishingPartitionManager.onPartitionFinishEvent("testToken"); // pendingFinish = true

        assertThat(finishingPartitionManager.getPendingFinishPartitions()).contains("testToken");

        // FinishPartitionWatchDog timeout fires — cancel instead of fatal failure
        finishingPartitionManager.cancelPendingFinish("testToken");

        assertThat(finishingPartitionManager.getPendingFinishPartitions()).doesNotContain("testToken");
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");

        // Partition can be re-registered for a new streaming cycle (updateToReadyForStreaming path)
        finishingPartitionManager.registerPartition("testToken");
        finishingPartitionManager.onPartitionFinishEvent("testToken"); // no emitted record this time → forceFinish
        Mockito.verify(consumer, Mockito.times(1)).accept("testToken");
    }

    @Test
    void multipleCommitNoEventsWithWrongCommitCallOnly() throws InterruptedException {
        BlockingConsumer<String> consumer = Mockito.mock(BlockingConsumer.class);
        SpannerConnectorConfig config = Mockito.mock(SpannerConnectorConfig.class);

        FinishingPartitionManager finishingPartitionManager = new FinishingPartitionManager(config, consumer);

        finishingPartitionManager.registerPartition("testToken");

        finishingPartitionManager.commitRecord("testToken", "recordUid3");

        // don't except call consumer.accept
        Mockito.verify(consumer, Mockito.times(0)).accept("testToken");
    }
}
