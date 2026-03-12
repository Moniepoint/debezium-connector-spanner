# Post Mortem: Interrupt Cascade During AFTER_COMMIT_NO_PARENT_WAIT Epoch Transition

**Date:** 2026-03-03
**Version:** 3.3.0.Final
**Strategy:** `AFTER_COMMIT_NO_PARENT_WAIT`
**Connector:** `mfb-core-journal-entries-avro-preprod-v1_task-0`

---

## Summary

After deploying the `AFTER_COMMIT_NO_PARENT_WAIT` finish strategy, production logs showed a storm of ERROR-level entries during epoch transitions — one pair per `READY_FOR_STREAMING` partition. The connector recovered normally, but the noise obscured real failures and filled alerting channels.

---

## Timeline

| Time (UTC) | Event |
|---|---|
| 15:40:17 | Initial partition (`Parent0`) finishes streaming; child partitions created with `startTimestamp=2026-03-03T15:40:17.968Z` |
| 15:42:55 | `NEW_EPOCH_STARTED` event fires; `stopProcessing()` interrupts the `TaskStateChangeEventProcessor` thread |
| 15:42:55 | ~10 pairs of ERROR log entries flood within the **same second** |
| 15:42:55+ | Task restarts cleanly; new task instance schedules child partitions successfully |

---

## Root Cause

### Background: What AFTER_COMMIT_NO_PARENT_WAIT changes

Under `AFTER_COMMIT` a partition waits for all parent commits before finishing, serialising the rebalance. Under `AFTER_COMMIT_NO_PARENT_WAIT` a partition marks itself finished immediately after its last record commits, without waiting for siblings. This makes epoch transitions happen **much more frequently and immediately**, which exposed a latent cascade bug in the offset retrieval path.

---

### The Cascade

The event handler thread (`SpannerConnector-TaskStateChangeEventProcessor`) processes a `NEW_EPOCH_STARTED` event while `stopProcessing()` is simultaneously called to restart the task. `stopProcessing()` calls `thread.interrupt()`, which plants the interrupt flag on the event handler thread.

```
stopProcessing() ──► thread.interrupt()
                             │
                             ▼
          ┌──────────────────────────────────────────────┐
          │  TaskStateChangeEventProcessor thread         │
          │                                              │
          │  processEvent(NEW_EPOCH_STARTED)             │
          │    └─ TakePartitionForStreamingOperation     │
          │         └─ forEach(READY_FOR_STREAMING)      │
          │              │                               │
          │         ┌────▼────────────────────────────┐  │
          │         │ Partition 1                     │  │
          │         │  retrieveOffsetMap()            │  │
          │         │    future.get(5s) ◄─────────────┼──┼── InterruptedException
          │         │    catch(InterruptedException)  │  │    (thread was interrupted)
          │         │    Thread.currentThread()       │  │
          │         │      .interrupt() ◄─────────────┼──┼── ★ interrupt flag SET
          │         │    return null                  │  │
          │         │  getOffset() → startTimestamp   │  │
          │         │  changeStream.submitPartition() │  │
          │         │    → false (stream stopped)     │  │
          │         │  ERROR logged                   │  │
          │         └────┬────────────────────────────┘  │
          │              │  forEach continues             │
          │         ┌────▼────────────────────────────┐  │
          │         │ Partition 2                     │  │
          │         │  retrieveOffsetMap()            │  │
          │         │    future.get(5s) ◄─────────────┼──┼── InterruptedException IMMEDIATELY
          │         │    (flag already set, no wait!) │  │    (no network call made)
          │         │    Thread.currentThread()       │  │
          │         │      .interrupt() ◄─────────────┼──┼── ★ interrupt flag RE-SET
          │         │    return null                  │  │
          │         │  ERROR logged                   │  │
          │         └────┬────────────────────────────┘  │
          │              │  forEach continues             │
          │         ┌────▼────────────────────────────┐  │
          │         │ Partition 3 … N  (same pattern) │  │
          │         │  2 × ERROR per partition        │  │
          │         └────────────────────────────────-┘  │
          │                                              │
          │  processEvent() returns normally             │
          │  while(!Thread.interrupted()) → flag clears  │
          │  loop exits → thread dies cleanly           │
          └──────────────────────────────────────────────┘
```

**Key mechanics of the cascade:**

1. `future.get()` on the calling (event handler) thread throws `InterruptedException` when that thread's interrupt flag is set.
2. The catch block calls `Thread.currentThread().interrupt()` to re-set the flag — correct Java practice.
3. The `forEach` lambda is **not interrupt-aware**: it continues to the next partition unconditionally.
4. Every subsequent `future.get()` call throws `InterruptedException` **instantly** (no 5-second wait, no network call) because the flag is already set.
5. This produces `2 × N` error log entries in the same millisecond, where N is the number of `READY_FOR_STREAMING` partitions.

### Why it wasn't visible before

Under `AFTER_COMMIT` the rebalance was infrequent and the task was not interrupted mid-event-processing. Under `AFTER_COMMIT_NO_PARENT_WAIT` the task stops and restarts on every partition finish, making the race between `stopProcessing()` and `processEvent()` a routine occurrence.

---

## Impact

- **Functional:** None. The child partitions remained in `READY_FOR_STREAMING` and were scheduled correctly by the new task instance after restart.
- **Observability:** ERROR-level log storm (~20+ entries per transition) triggered false alerts and made it harder to spot real failures in the same log stream.

---

## Fix

Two minimal changes, no behavioural regression:

### 1. `TakePartitionForStreamingOperation.java` — break on interrupt

**Before:**
```java
toStreaming.forEach(partitionState -> {
    LOGGER.info("Task {}, submitting the partition for streaming {}", ...);
    if (this.submitPartition(partitionState, taskSyncContext)) {
        toSchedule.add(partitionState.getToken());
    } else {
        LOGGER.error("Task {}, failed to submit partition {} with state {}", ...);
    }
});
```

**After:**
```java
for (PartitionState partitionState : toStreaming) {
    if (Thread.currentThread().isInterrupted()) {
        LOGGER.info("Task {}, stopping partition submission - thread was interrupted", ...);
        break;
    }
    LOGGER.info("Task {}, submitting the partition for streaming {}", ...);
    if (this.submitPartition(partitionState, taskSyncContext)) {
        toSchedule.add(partitionState.getToken());
    } else {
        LOGGER.error("Task {}, failed to submit partition {} with state {}", ...);
    }
}
```

The interrupt flag is checked **before** each partition. After the first partition sets the flag (via `retrieveOffsetMap()`'s catch block), the loop breaks instead of attempting all remaining partitions.

### 2. `PartitionOffsetProvider.java` — downgrade log level

**Before:**
```java
LOGGER.error("Token {},interrupting PartitionOffsetProvider", spannerPartition, e);
```

**After:**
```java
LOGGER.warn("Token {},interrupting PartitionOffsetProvider", spannerPartition, e);
```

The interrupt is intentional (task shutting down for epoch transition). ERROR implies an unexpected failure; WARN correctly signals "expected-but-notable" shutdown noise.

---

## Result After Fix

```
stopProcessing() ──► thread.interrupt()
                             │
          ┌──────────────────────────────────────────────┐
          │  TaskStateChangeEventProcessor thread         │
          │                                              │
          │  processEvent(NEW_EPOCH_STARTED)             │
          │    └─ TakePartitionForStreamingOperation     │
          │         └─ for(READY_FOR_STREAMING)          │
          │              │                               │
          │         ┌────▼────────────────────────────┐  │
          │         │ Partition 1                     │  │
          │         │  isInterrupted()? → false       │  │
          │         │  retrieveOffsetMap()            │  │
          │         │    future.get() → Interrupted   │  │
          │         │    WARN logged (1 entry)        │  │
          │         │    interrupt flag SET           │  │
          │         │  submitPartition() → false      │  │
          │         │  ERROR logged (1 entry)         │  │
          │         └────┬────────────────────────────┘  │
          │              │                               │
          │         ┌────▼────────────────────────────┐  │
          │         │ Partition 2                     │  │
          │         │  isInterrupted()? → TRUE        │  │
          │         │  INFO logged, break ◄───────────┼──┼── loop stops here
          │         └────────────────────────────────-┘  │
          │                                              │
          │  Partitions 3…N: not processed, stay        │
          │  READY_FOR_STREAMING for next task instance  │
          └──────────────────────────────────────────────┘
```

**Log volume reduction:** from `2 × N` ERROR entries to `1 WARN + 1 ERROR + 1 INFO` per transition regardless of partition count.

---

## Lessons Learned

- **`forEach` with lambdas is not interrupt-aware.** When a loop body can set the thread interrupt flag (directly or via a called method), use a `for` loop with an explicit `isInterrupted()` check.
- **Log level accuracy matters for on-call.** An expected shutdown sequence logged at ERROR level trains responders to ignore ERRORs, degrading incident response for real failures.
- **Higher-frequency strategies surface latent concurrency bugs.** Any strategy that increases task restart frequency should be stress-tested specifically for races between `stopProcessing()` and in-flight event processing.
