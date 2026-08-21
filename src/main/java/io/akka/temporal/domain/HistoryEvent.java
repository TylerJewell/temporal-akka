package io.akka.temporal.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import java.util.Map;

/**
 * One event of a workflow's persisted history.
 *
 * <p>The four fields every event carries sit on the record and the type-specific fields sit in
 * {@link Attributes}, mirroring the shape the source persists: a common header and one of many
 * attribute bodies.
 *
 * <p>Jackson type information is on {@link Attributes} rather than only Akka's {@code @TypeName}
 * because a polymorphic type nested inside a <em>command</em> is deserialized by Jackson alone, and
 * without it the command is rejected before any component sees it (question-log row T1).
 */
public record HistoryEvent(long eventId, long version, long taskId, Attributes attributes) {

  public String typeName() {
    return attributes.typeName();
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionStarted.class, name = "WorkflowExecutionStarted"),
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionCompleted.class, name = "WorkflowExecutionCompleted"),
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionFailed.class, name = "WorkflowExecutionFailed"),
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionTimedOut.class, name = "WorkflowExecutionTimedOut"),
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionTerminated.class, name = "WorkflowExecutionTerminated"),
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionCanceled.class, name = "WorkflowExecutionCanceled"),
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionContinuedAsNew.class, name = "WorkflowExecutionContinuedAsNew"),
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionSignaled.class, name = "WorkflowExecutionSignaled"),
    @JsonSubTypes.Type(value = Attributes.WorkflowExecutionCancelRequested.class, name = "WorkflowExecutionCancelRequested"),
    @JsonSubTypes.Type(value = Attributes.WorkflowTaskScheduled.class, name = "WorkflowTaskScheduled"),
    @JsonSubTypes.Type(value = Attributes.WorkflowTaskStarted.class, name = "WorkflowTaskStarted"),
    @JsonSubTypes.Type(value = Attributes.WorkflowTaskCompleted.class, name = "WorkflowTaskCompleted"),
    @JsonSubTypes.Type(value = Attributes.WorkflowTaskFailed.class, name = "WorkflowTaskFailed"),
    @JsonSubTypes.Type(value = Attributes.WorkflowTaskTimedOut.class, name = "WorkflowTaskTimedOut"),
    @JsonSubTypes.Type(value = Attributes.ActivityTaskScheduled.class, name = "ActivityTaskScheduled"),
    @JsonSubTypes.Type(value = Attributes.ActivityTaskStarted.class, name = "ActivityTaskStarted"),
    @JsonSubTypes.Type(value = Attributes.ActivityTaskCompleted.class, name = "ActivityTaskCompleted"),
    @JsonSubTypes.Type(value = Attributes.ActivityTaskFailed.class, name = "ActivityTaskFailed"),
    @JsonSubTypes.Type(value = Attributes.ActivityTaskTimedOut.class, name = "ActivityTaskTimedOut"),
    @JsonSubTypes.Type(value = Attributes.ActivityTaskCancelRequested.class, name = "ActivityTaskCancelRequested"),
    @JsonSubTypes.Type(value = Attributes.ActivityTaskCanceled.class, name = "ActivityTaskCanceled"),
    @JsonSubTypes.Type(value = Attributes.TimerStarted.class, name = "TimerStarted"),
    @JsonSubTypes.Type(value = Attributes.TimerFired.class, name = "TimerFired"),
    @JsonSubTypes.Type(value = Attributes.TimerCanceled.class, name = "TimerCanceled"),
    @JsonSubTypes.Type(value = Attributes.StartChildWorkflowExecutionInitiated.class, name = "StartChildWorkflowExecutionInitiated"),
    @JsonSubTypes.Type(value = Attributes.StartChildWorkflowExecutionFailed.class, name = "StartChildWorkflowExecutionFailed"),
    @JsonSubTypes.Type(value = Attributes.ChildWorkflowExecutionStarted.class, name = "ChildWorkflowExecutionStarted"),
    @JsonSubTypes.Type(value = Attributes.ChildWorkflowExecutionCompleted.class, name = "ChildWorkflowExecutionCompleted"),
    @JsonSubTypes.Type(value = Attributes.ChildWorkflowExecutionFailed.class, name = "ChildWorkflowExecutionFailed"),
    @JsonSubTypes.Type(value = Attributes.ChildWorkflowExecutionCanceled.class, name = "ChildWorkflowExecutionCanceled"),
    @JsonSubTypes.Type(value = Attributes.ChildWorkflowExecutionTimedOut.class, name = "ChildWorkflowExecutionTimedOut"),
    @JsonSubTypes.Type(value = Attributes.ChildWorkflowExecutionTerminated.class, name = "ChildWorkflowExecutionTerminated"),
    @JsonSubTypes.Type(value = Attributes.RequestCancelExternalWorkflowExecutionInitiated.class, name = "RequestCancelExternalWorkflowExecutionInitiated"),
    @JsonSubTypes.Type(value = Attributes.SignalExternalWorkflowExecutionInitiated.class, name = "SignalExternalWorkflowExecutionInitiated"),
    @JsonSubTypes.Type(value = Attributes.MarkerRecorded.class, name = "MarkerRecorded"),
    @JsonSubTypes.Type(value = Attributes.UpsertWorkflowSearchAttributes.class, name = "UpsertWorkflowSearchAttributes"),
    @JsonSubTypes.Type(value = Attributes.Unknown.class, name = "Unknown")
  })
  public sealed interface Attributes {

    /** The name this attribute body would carry in the source's event-type enumeration. */
    default String typeName() {
      return getClass().getSimpleName();
    }

    record WorkflowExecutionStarted(String workflowType, String taskQueue, String firstExecutionRunId, int attempt)
        implements Attributes {}

    record WorkflowExecutionCompleted(long workflowTaskCompletedEventId) implements Attributes {}

    record WorkflowExecutionFailed(long workflowTaskCompletedEventId, String failure) implements Attributes {}

    record WorkflowExecutionTimedOut(String retryState) implements Attributes {}

    record WorkflowExecutionTerminated(String reason, String identity) implements Attributes {}

    record WorkflowExecutionCanceled(long workflowTaskCompletedEventId) implements Attributes {}

    record WorkflowExecutionContinuedAsNew(String newExecutionRunId, String workflowType, String taskQueue,
                                           long workflowTaskCompletedEventId) implements Attributes {}

    record WorkflowExecutionSignaled(String signalName, String identity) implements Attributes {}

    record WorkflowExecutionCancelRequested(String identity) implements Attributes {}

    record WorkflowTaskScheduled(String taskQueue, long startToCloseTimeoutMillis, int attempt) implements Attributes {}

    record WorkflowTaskStarted(long scheduledEventId, String identity, String requestId) implements Attributes {}

    record WorkflowTaskCompleted(long scheduledEventId, long startedEventId, String identity) implements Attributes {}

    record WorkflowTaskFailed(long scheduledEventId, long startedEventId, String cause) implements Attributes {}

    /**
     * {@code timeoutType} decides whether an attempt is charged: schedule-to-start is the one
     * timeout that does not, and so leaves no synthesised task behind (R21).
     */
    record WorkflowTaskTimedOut(long scheduledEventId, long startedEventId, String timeoutType) implements Attributes {}

    record ActivityTaskScheduled(String activityId, String activityType, String taskQueue,
                                 long workflowTaskCompletedEventId) implements Attributes {}

    record ActivityTaskStarted(long scheduledEventId, String identity, String requestId, int attempt)
        implements Attributes {}

    record ActivityTaskCompleted(long scheduledEventId, long startedEventId, String identity) implements Attributes {}

    record ActivityTaskFailed(long scheduledEventId, long startedEventId, String identity) implements Attributes {}

    record ActivityTaskTimedOut(long scheduledEventId, long startedEventId, String retryState) implements Attributes {}

    record ActivityTaskCancelRequested(long scheduledEventId, long workflowTaskCompletedEventId)
        implements Attributes {}

    record ActivityTaskCanceled(long scheduledEventId, long startedEventId, long latestCancelRequestedEventId)
        implements Attributes {}

    record TimerStarted(String timerId, long startToFireTimeoutMillis, long workflowTaskCompletedEventId)
        implements Attributes {}

    record TimerFired(String timerId, long startedEventId) implements Attributes {}

    record TimerCanceled(String timerId, long startedEventId, long workflowTaskCompletedEventId)
        implements Attributes {}

    record StartChildWorkflowExecutionInitiated(String namespace, String workflowId, String workflowType,
                                                String taskQueue, long workflowTaskCompletedEventId)
        implements Attributes {}

    record StartChildWorkflowExecutionFailed(long initiatedEventId, String workflowId, String cause)
        implements Attributes {}

    record ChildWorkflowExecutionStarted(long initiatedEventId, String workflowId, String runId, String workflowType)
        implements Attributes {}

    record ChildWorkflowExecutionCompleted(long initiatedEventId, long startedEventId, String workflowId)
        implements Attributes {}

    record ChildWorkflowExecutionFailed(long initiatedEventId, long startedEventId, String workflowId,
                                        String retryState) implements Attributes {}

    record ChildWorkflowExecutionCanceled(long initiatedEventId, long startedEventId, String workflowId)
        implements Attributes {}

    record ChildWorkflowExecutionTimedOut(long initiatedEventId, long startedEventId, String workflowId,
                                          String retryState) implements Attributes {}

    record ChildWorkflowExecutionTerminated(long initiatedEventId, long startedEventId, String workflowId)
        implements Attributes {}

    record RequestCancelExternalWorkflowExecutionInitiated(String namespace, String workflowId, String runId,
                                                           long workflowTaskCompletedEventId) implements Attributes {}

    record SignalExternalWorkflowExecutionInitiated(String namespace, String workflowId, String runId,
                                                    String signalName, long workflowTaskCompletedEventId)
        implements Attributes {}

    record MarkerRecorded(String markerName, long workflowTaskCompletedEventId) implements Attributes {}

    record UpsertWorkflowSearchAttributes(Map<String, String> indexedFields, long workflowTaskCompletedEventId)
        implements Attributes {}

    /** An event type replay does not know. Rejected rather than ignored (R20). */
    record Unknown(String type) implements Attributes {
      @Override
      public String typeName() {
        return type;
      }
    }
  }

  /** A persistence batch: a non-empty, ordered run of events that were written together. */
  public record Batch(List<HistoryEvent> events) {}
}
