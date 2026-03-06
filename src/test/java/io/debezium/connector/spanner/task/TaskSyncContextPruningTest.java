/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Verifies that buildNewEpochTaskSyncEvent() and buildUpdateEpochTaskSyncEvent() prune
 * terminal partition states before serialisation to keep sync-topic messages small.
 *
 * Pruning rules:
 *  - REMOVED  → always dropped
 *  - FINISHED → dropped unless at least one CREATED partition anywhere lists this token as a parent
 *  - All other states (CREATED, READY_FOR_STREAMING, SCHEDULED, RUNNING) → always kept
 *
 * REGULAR and REBALANCE_ANSWER messages are not pruned (they carry only the current task's
 * own partitions and are small by construction).
 */
class TaskSyncContextPruningTest {

    // -------------------------------------------------------------------------
    // 1. REMOVED always stripped
    // -------------------------------------------------------------------------
    @Test
    void removedPartitionsAreAlwaysDroppedFromEpochMessages() {
        TaskState task = task("t0",
                partition("removed-1", PartitionStateEnum.REMOVED),
                partition("removed-2", PartitionStateEnum.REMOVED));

        TaskSyncContext ctx = context("t0", task, Map.of());

        assertTokensInEpochMessages(ctx, "t0", Set.of());
    }

    // -------------------------------------------------------------------------
    // 2. FINISHED with no CREATED child → stripped
    // -------------------------------------------------------------------------
    @Test
    void finishedPartitionWithNoCreatedChildIsDroppedFromEpochMessages() {
        TaskState task = task("t0",
                partition("finished-orphan", PartitionStateEnum.FINISHED),
                partition("running-1", PartitionStateEnum.RUNNING));

        TaskSyncContext ctx = context("t0", task, Map.of());

        Set<String> tokens = tokensInEpochMessages(ctx, "t0");
        assertThat(tokens).doesNotContain("finished-orphan");
        assertThat(tokens).contains("running-1");
    }

    // -------------------------------------------------------------------------
    // 3. FINISHED with CREATED child → kept
    // -------------------------------------------------------------------------
    @Test
    void finishedPartitionReferencedByCreatedChildIsKeptInEpochMessages() {
        PartitionState finishedParent = partition("parent-token", PartitionStateEnum.FINISHED);
        PartitionState createdChild = PartitionState.builder()
                .token("child-token")
                .state(PartitionStateEnum.CREATED)
                .parents(Set.of("parent-token"))
                .build();

        TaskState task = task("t0", finishedParent, createdChild);
        TaskSyncContext ctx = context("t0", task, Map.of());

        assertThat(tokensInEpochMessages(ctx, "t0")).contains("parent-token", "child-token");
    }

    // -------------------------------------------------------------------------
    // 4. Active states always kept
    // -------------------------------------------------------------------------
    @Test
    void activeStatesAreNeverDropped() {
        TaskState task = task("t0",
                partition("p-created", PartitionStateEnum.CREATED),
                partition("p-ready", PartitionStateEnum.READY_FOR_STREAMING),
                partition("p-scheduled", PartitionStateEnum.SCHEDULED),
                partition("p-running", PartitionStateEnum.RUNNING));

        TaskSyncContext ctx = context("t0", task, Map.of());

        Set<String> tokens = tokensInEpochMessages(ctx, "t0");
        assertThat(tokens).containsExactlyInAnyOrder("p-created", "p-ready", "p-scheduled", "p-running");
    }

    // -------------------------------------------------------------------------
    // 5. REGULAR message is unaffected (FINISHED/REMOVED kept in own-task view)
    // -------------------------------------------------------------------------
    @Test
    void regularMessageCarriesTerminalPartitionsUnpruned() {
        TaskState task = task("t0",
                partition("fin", PartitionStateEnum.FINISHED),
                partition("rem", PartitionStateEnum.REMOVED),
                partition("run", PartitionStateEnum.RUNNING));

        TaskSyncContext ctx = context("t0", task, Map.of());

        TaskSyncEvent regular = ctx.buildCurrentTaskSyncEvent();
        Set<String> tokens = tokenSet(regular.getTaskStates().get("t0").getPartitions());
        assertThat(tokens).contains("fin", "rem", "run");
    }

    // -------------------------------------------------------------------------
    // 6. Cross-task parent lookup — neededParents built from ALL tasks' CREATED
    // -------------------------------------------------------------------------
    @Test
    void finishedPartitionNeededByCreatedChildInAnotherTaskIsKept() {
        // task0 has the FINISHED parent
        TaskState task0 = task("t0",
                partition("parent-token", PartitionStateEnum.FINISHED));

        // task1 has the CREATED child that needs the parent
        PartitionState createdChild = PartitionState.builder()
                .token("child-token")
                .state(PartitionStateEnum.CREATED)
                .parents(Set.of("parent-token"))
                .build();
        TaskState task1 = task("t1", createdChild);

        TaskSyncContext ctx = context("t0", task0, Map.of("t1", task1));

        // parent-token must survive in task0's wire state because task1 needs it
        assertThat(tokensInEpochMessages(ctx, "t0")).contains("parent-token");
    }

    @Test
    void finishedPartitionNotNeededByAnyOtherTaskIsDropped() {
        TaskState task0 = task("t0",
                partition("orphan-finished", PartitionStateEnum.FINISHED));

        // task1 has a CREATED child but its parent is different
        PartitionState createdChild = PartitionState.builder()
                .token("child-token")
                .state(PartitionStateEnum.CREATED)
                .parents(Set.of("some-other-parent"))
                .build();
        TaskState task1 = task("t1", createdChild);

        TaskSyncContext ctx = context("t0", task0, Map.of("t1", task1));

        assertThat(tokensInEpochMessages(ctx, "t0")).doesNotContain("orphan-finished");
    }

    // -------------------------------------------------------------------------
    // helpers
    // -------------------------------------------------------------------------

    private static PartitionState partition(String token, PartitionStateEnum state) {
        return PartitionState.builder().token(token).state(state).build();
    }

    private static TaskState task(String uid, PartitionState... partitions) {
        return TaskState.builder()
                .taskUid(uid)
                .partitions(Arrays.asList(partitions))
                .sharedPartitions(List.of())
                .build();
    }

    private static TaskSyncContext context(String ownerUid, TaskState currentTask,
                                           Map<String, TaskState> otherTasks) {
        return TaskSyncContext.builder()
                .taskUid(ownerUid)
                .currentTaskState(currentTask)
                .taskStates(otherTasks)
                .build();
    }

    /** Returns token set from BOTH new-epoch and update-epoch; asserts they are identical. */
    private static Set<String> tokensInEpochMessages(TaskSyncContext ctx, String taskUid) {
        Set<String> fromNew = tokenSet(ctx.buildNewEpochTaskSyncEvent()
                .getTaskStates().get(taskUid).getPartitions());
        Set<String> fromUpdate = tokenSet(ctx.buildUpdateEpochTaskSyncEvent()
                .getTaskStates().get(taskUid).getPartitions());
        assertThat(fromNew).as("NEW_EPOCH and UPDATE_EPOCH must produce identical pruned states")
                .isEqualTo(fromUpdate);
        return fromNew;
    }

    private static void assertTokensInEpochMessages(TaskSyncContext ctx, String taskUid,
                                                    Set<String> expected) {
        assertThat(tokensInEpochMessages(ctx, taskUid)).isEqualTo(expected);
    }

    private static Set<String> tokenSet(Collection<PartitionState> partitions) {
        return partitions.stream().map(PartitionState::getToken).collect(Collectors.toSet());
    }
}
