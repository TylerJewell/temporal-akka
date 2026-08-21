package io.akka.temporal.domain;

import io.akka.temporal.domain.HistoryEvent.Attributes;
import java.util.List;
import java.util.Map;

/**
 * Event constructors shaped exactly like the ones the source was driven through in
 * {@code temporal-port/probes/replay-harness/harness.go}, so a history written here and a history
 * written there are the same history.
 */
public final class Histories {

  private Histories() {}

  /** Task ids follow the harness's convention so both sides carry the same value. */
  private static HistoryEvent ev(long id, long version, Attributes attributes) {
    return new HistoryEvent(id, version, id * 10, attributes);
  }

  public static HistoryEvent.Batch batch(List<HistoryEvent> events) {
    return new HistoryEvent.Batch(events);
  }

  public static HistoryEvent started(long id, long version) {
    return ev(id, version, new Attributes.WorkflowExecutionStarted("T", "tq", "run-1", 1));
  }

  public static HistoryEvent wftScheduled(long id, long version) {
    return ev(id, version, new Attributes.WorkflowTaskScheduled("tq", 60_000, 1));
  }

  public static HistoryEvent wftScheduledAtAttempt(long id, long version, int attempt) {
    return ev(id, version, new Attributes.WorkflowTaskScheduled("tq", 60_000, attempt));
  }

  public static HistoryEvent wftStarted(long id, long version, long scheduledId) {
    return ev(id, version, new Attributes.WorkflowTaskStarted(scheduledId, "worker", "wft-req"));
  }

  public static HistoryEvent wftCompleted(long id, long version, long scheduledId, long startedId) {
    return ev(id, version, new Attributes.WorkflowTaskCompleted(scheduledId, startedId, "worker"));
  }

  public static HistoryEvent wftFailed(long id, long version, long scheduledId, long startedId) {
    return ev(id, version, new Attributes.WorkflowTaskFailed(scheduledId, startedId, "UnhandledCommand"));
  }

  public static HistoryEvent wftTimedOut(long id, long version, long scheduledId, long startedId, String timeoutType) {
    return ev(id, version, new Attributes.WorkflowTaskTimedOut(scheduledId, startedId, timeoutType));
  }

  public static HistoryEvent activityScheduled(long id, long version, String activityId) {
    return ev(id, version, new Attributes.ActivityTaskScheduled(activityId, "A", "tq", id - 1));
  }

  public static HistoryEvent activityStarted(long id, long version, long scheduledId) {
    return ev(id, version, new Attributes.ActivityTaskStarted(scheduledId, "worker", "act-req", 1));
  }

  public static HistoryEvent activityCompleted(long id, long version, long scheduledId, long startedId) {
    return ev(id, version, new Attributes.ActivityTaskCompleted(scheduledId, startedId, "worker"));
  }

  public static HistoryEvent activityFailed(long id, long version, long scheduledId, long startedId) {
    return ev(id, version, new Attributes.ActivityTaskFailed(scheduledId, startedId, "worker"));
  }

  public static HistoryEvent activityTimedOut(long id, long version, long scheduledId, long startedId) {
    return ev(id, version, new Attributes.ActivityTaskTimedOut(scheduledId, startedId, "Timeout"));
  }

  public static HistoryEvent activityCancelRequested(long id, long version, long scheduledId) {
    return ev(id, version, new Attributes.ActivityTaskCancelRequested(scheduledId, id - 1));
  }

  public static HistoryEvent activityCanceled(long id, long version, long scheduledId, long startedId) {
    return ev(id, version, new Attributes.ActivityTaskCanceled(scheduledId, startedId, scheduledId));
  }

  public static HistoryEvent timerStarted(long id, long version, String timerId) {
    return ev(id, version, new Attributes.TimerStarted(timerId, 60_000, id - 1));
  }

  public static HistoryEvent timerFired(long id, long version, String timerId, long startedId) {
    return ev(id, version, new Attributes.TimerFired(timerId, startedId));
  }

  public static HistoryEvent timerCanceled(long id, long version, String timerId, long startedId) {
    return ev(id, version, new Attributes.TimerCanceled(timerId, startedId, id - 1));
  }

  public static HistoryEvent childInitiated(long id, long version, String childWorkflowId) {
    return ev(id, version,
        new Attributes.StartChildWorkflowExecutionInitiated("ns", childWorkflowId, "C", "tq", id - 1));
  }

  public static HistoryEvent childStartFailed(long id, long version, long initiatedId, String childWorkflowId) {
    return ev(id, version,
        new Attributes.StartChildWorkflowExecutionFailed(initiatedId, childWorkflowId, "WorkflowAlreadyExists"));
  }

  public static HistoryEvent childStarted(long id, long version, long initiatedId, String childWorkflowId,
                                          String childRunId) {
    return ev(id, version, new Attributes.ChildWorkflowExecutionStarted(initiatedId, childWorkflowId, childRunId, "C"));
  }

  public static HistoryEvent childCompleted(long id, long version, long initiatedId, long startedId,
                                            String childWorkflowId) {
    return ev(id, version, new Attributes.ChildWorkflowExecutionCompleted(initiatedId, startedId, childWorkflowId));
  }

  public static HistoryEvent childFailed(long id, long version, long initiatedId, long startedId,
                                         String childWorkflowId) {
    return ev(id, version,
        new Attributes.ChildWorkflowExecutionFailed(initiatedId, startedId, childWorkflowId, "MaximumAttemptsReached"));
  }

  public static HistoryEvent childCanceled(long id, long version, long initiatedId, long startedId,
                                           String childWorkflowId) {
    return ev(id, version, new Attributes.ChildWorkflowExecutionCanceled(initiatedId, startedId, childWorkflowId));
  }

  public static HistoryEvent childTimedOut(long id, long version, long initiatedId, long startedId,
                                           String childWorkflowId) {
    return ev(id, version,
        new Attributes.ChildWorkflowExecutionTimedOut(initiatedId, startedId, childWorkflowId, "Timeout"));
  }

  public static HistoryEvent childTerminated(long id, long version, long initiatedId, long startedId,
                                             String childWorkflowId) {
    return ev(id, version, new Attributes.ChildWorkflowExecutionTerminated(initiatedId, startedId, childWorkflowId));
  }

  public static HistoryEvent signaled(long id, long version, String signalName) {
    return ev(id, version, new Attributes.WorkflowExecutionSignaled(signalName, "client"));
  }

  public static HistoryEvent cancelRequested(long id, long version) {
    return ev(id, version, new Attributes.WorkflowExecutionCancelRequested("client"));
  }

  public static HistoryEvent cancelExternalInitiated(long id, long version, String targetWorkflowId) {
    return ev(id, version, new Attributes.RequestCancelExternalWorkflowExecutionInitiated("ns", targetWorkflowId,
        "target-run", id - 1));
  }

  public static HistoryEvent signalExternalInitiated(long id, long version, String targetWorkflowId) {
    return ev(id, version, new Attributes.SignalExternalWorkflowExecutionInitiated("ns", targetWorkflowId,
        "target-run", "s", id - 1));
  }

  public static HistoryEvent markerRecorded(long id, long version, String markerName) {
    return ev(id, version, new Attributes.MarkerRecorded(markerName, id - 1));
  }

  public static HistoryEvent upsertSearchAttributes(long id, long version, Map<String, String> fields) {
    return ev(id, version, new Attributes.UpsertWorkflowSearchAttributes(fields, id - 1));
  }

  public static HistoryEvent completed(long id, long version, long wftCompletedId) {
    return ev(id, version, new Attributes.WorkflowExecutionCompleted(wftCompletedId));
  }

  public static HistoryEvent failed(long id, long version, long wftCompletedId) {
    return ev(id, version, new Attributes.WorkflowExecutionFailed(wftCompletedId, "boom"));
  }

  public static HistoryEvent timedOut(long id, long version) {
    return ev(id, version, new Attributes.WorkflowExecutionTimedOut("Timeout"));
  }

  public static HistoryEvent terminated(long id, long version) {
    return ev(id, version, new Attributes.WorkflowExecutionTerminated("term", "client"));
  }

  public static HistoryEvent canceled(long id, long version, long wftCompletedId) {
    return ev(id, version, new Attributes.WorkflowExecutionCanceled(wftCompletedId));
  }

  public static HistoryEvent continuedAsNew(long id, long version, long wftCompletedId, String newRunId) {
    return ev(id, version, new Attributes.WorkflowExecutionContinuedAsNew(newRunId, "T", "tq", wftCompletedId));
  }

  public static HistoryEvent unknown(long id, long version, String type) {
    return ev(id, version, new Attributes.Unknown(type));
  }
}
