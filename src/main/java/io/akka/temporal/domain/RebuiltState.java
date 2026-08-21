package io.akka.temporal.domain;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * What a workflow's history replays to.
 *
 * <p>Immutable: every rule in SPEC-001 §3 is a function from one of these and one event to the
 * next one, which is what lets the same fold drive both the entity's journal replay and the
 * benchmark's in-memory run.
 *
 * <p>The pending maps are sorted, so two states built from the same events compare equal field for
 * field however the events reached them.
 */
public record RebuiltState(
    Status status,
    RunState runState,
    long nextEventId,
    long lastFirstEventId,
    long currentVersion,
    String workflowTypeName,
    String taskQueue,
    long lastCompletedWorkflowTaskStartedEventId,
    Map<Long, PendingActivity> pendingActivities,
    Map<String, PendingTimer> pendingTimers,
    Map<Long, PendingChild> pendingChildren,
    Map<Long, PendingExternal> pendingCancelExternal,
    Map<Long, PendingExternal> pendingSignalExternal,
    WorkflowTask workflowTask,
    List<VersionItem> versionHistory,
    long signalCount,
    boolean cancelRequested,
    long completionEventBatchId,
    Map<String, String> searchAttributes,
    long replayEventBatchId) {

  public enum Status {
    Running, Completed, Failed, TimedOut, Terminated, Canceled, ContinuedAsNew
  }

  public enum RunState {
    Created, Running, Completed
  }

  public record PendingActivity(long scheduledEventId, long startedEventId, String activityId,
                                String activityType, int attempt, boolean cancelRequested) {}

  public record PendingTimer(String timerId, long startedEventId) {}

  public record PendingChild(long initiatedEventId, long startedEventId, String workflowId,
                             String workflowTypeName) {}

  /** {@code requestId} is derived from the initiating event rather than minted (SPEC-001 D1). */
  public record PendingExternal(long initiatedEventId, String workflowId, String runId, String requestId) {}

  /** Absent is represented by a null {@code workflowTask}, matching the source's scheduled-id-zero. */
  public record WorkflowTask(long scheduledEventId, long startedEventId, int attempt, String taskQueue,
                             String requestId) {}

  public record VersionItem(long eventId, long version) {}

  /**
   * The state a rebuild starts from. {@code nextEventId} is 1 because a never-persisted state's
   * next event id is the first event id, and that value is what R5 makes readable mid-batch.
   */
  public static RebuiltState empty() {
    return new RebuiltState(Status.Running, RunState.Created, 1L, 0L, 0L, "", "", 0L,
        Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), null, List.of(), 0L, false, 0L, Map.of(), 0L);
  }

  // ---- copy helpers, one per group of fields the rules move together ----

  RebuiltState with(Status status, RunState runState) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers, pendingChildren,
        pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withPosition(long nextEventId, long lastFirstEventId, long currentVersion, long replayEventBatchId,
                            List<VersionItem> versionHistory) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers, pendingChildren,
        pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withNextEventId(long nextEventId) {
    return withPosition(nextEventId, lastFirstEventId, currentVersion, replayEventBatchId, versionHistory);
  }

  RebuiltState withStart(String workflowTypeName, String taskQueue) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion,
        workflowTypeName, taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers,
        pendingChildren, pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount,
        cancelRequested, completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withWorkflowTask(WorkflowTask task, long lastCompletedStartedEventId) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedStartedEventId, pendingActivities, pendingTimers, pendingChildren,
        pendingCancelExternal, pendingSignalExternal, task, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withActivities(Map<Long, PendingActivity> activities) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, activities, pendingTimers, pendingChildren,
        pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withTimers(Map<String, PendingTimer> timers) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, timers, pendingChildren,
        pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withChildren(Map<Long, PendingChild> children) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers, children,
        pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withExternals(Map<Long, PendingExternal> cancels, Map<Long, PendingExternal> signals) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers, pendingChildren,
        cancels, signals, workflowTask, versionHistory, signalCount, cancelRequested, completionEventBatchId,
        searchAttributes, replayEventBatchId);
  }

  RebuiltState withSignalCount(long signalCount) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers, pendingChildren,
        pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withCancelRequested(boolean cancelRequested) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers, pendingChildren,
        pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withCompletion(Status status, long completionEventBatchId) {
    return new RebuiltState(status, RunState.Completed, nextEventId, lastFirstEventId, currentVersion,
        workflowTypeName, taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers,
        pendingChildren, pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount,
        cancelRequested, completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  RebuiltState withSearchAttributes(Map<String, String> searchAttributes) {
    return new RebuiltState(status, runState, nextEventId, lastFirstEventId, currentVersion, workflowTypeName,
        taskQueue, lastCompletedWorkflowTaskStartedEventId, pendingActivities, pendingTimers, pendingChildren,
        pendingCancelExternal, pendingSignalExternal, workflowTask, versionHistory, signalCount, cancelRequested,
        completionEventBatchId, searchAttributes, replayEventBatchId);
  }

  static <V> Map<Long, V> put(Map<Long, V> from, long key, V value) {
    var next = new TreeMap<>(from);
    next.put(key, value);
    return next;
  }

  static <V> Map<Long, V> remove(Map<Long, V> from, long key) {
    if (!from.containsKey(key)) {
      return from;
    }
    var next = new TreeMap<>(from);
    next.remove(key);
    return next;
  }

  static Map<String, PendingTimer> putTimer(Map<String, PendingTimer> from, String key, PendingTimer value) {
    var next = new LinkedHashMap<>(from);
    next.put(key, value);
    return next;
  }

  static Map<String, PendingTimer> removeTimer(Map<String, PendingTimer> from, String key) {
    if (!from.containsKey(key)) {
      return from;
    }
    var next = new LinkedHashMap<>(from);
    next.remove(key);
    return next;
  }
}
