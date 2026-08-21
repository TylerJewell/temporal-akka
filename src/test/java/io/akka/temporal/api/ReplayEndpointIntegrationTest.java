package io.akka.temporal.api;

import static org.assertj.core.api.Assertions.assertThat;

import akka.javasdk.testkit.TestKitSupport;
import io.akka.temporal.domain.HistoryEvent;
import io.akka.temporal.domain.HistoryEvent.Attributes;
import io.akka.temporal.domain.RebuiltState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * SPEC-001 D4 and D5 — the capability reached the way an outside caller reaches it, one
 * persistence batch per call, with every event type crossing both the command boundary and the
 * journal on the way.
 */
public class ReplayEndpointIntegrationTest extends TestKitSupport {

  private static HistoryEvent ev(long id, Attributes attributes) {
    return new HistoryEvent(id, 0, id * 10, attributes);
  }

  private RebuiltState post(String workflowId, String runId, List<HistoryEvent> events) {
    return httpClient
        .POST("/replay/" + workflowId + "/" + runId + "/batches")
        .withRequestBody(new HistoryEvent.Batch(events))
        .responseBodyAs(RebuiltState.class)
        .invoke()
        .body();
  }

  private RebuiltState get(String workflowId, String runId) {
    return httpClient
        .GET("/replay/" + workflowId + "/" + runId)
        .responseBodyAs(RebuiltState.class)
        .invoke()
        .body();
  }

  @Test
  public void replaysAHistoryOneBatchPerCallAndReadsItBack() {
    post("wf-1", "run-1", List.of(
        ev(1, new Attributes.WorkflowExecutionStarted("T", "tq", "run-1", 1)),
        ev(2, new Attributes.WorkflowTaskScheduled("tq", 60_000, 1))));
    post("wf-1", "run-1", List.of(
        ev(3, new Attributes.WorkflowTaskStarted(2, "worker", "wft-req")),
        ev(4, new Attributes.WorkflowTaskCompleted(2, 3, "worker"))));
    var afterThird = post("wf-1", "run-1", List.of(
        ev(5, new Attributes.ActivityTaskScheduled("a1", "A", "tq", 4)),
        ev(6, new Attributes.ActivityTaskStarted(5, "worker", "act-req", 1))));

    assertThat(afterThird.nextEventId()).isEqualTo(7L);
    assertThat(afterThird.lastFirstEventId()).isEqualTo(5L);
    assertThat(afterThird.pendingActivities()).hasSize(1);

    var read = get("wf-1", "run-1");
    assertThat(read).isEqualTo(afterThird);
  }

  @Test
  public void everyEventTypeSurvivesTheCommandAndTheJournal() {
    // One of each attribute body in SPEC-001 §1, in an order that keeps the references valid.
    var events = new ArrayList<HistoryEvent>();
    events.add(ev(1, new Attributes.WorkflowExecutionStarted("T", "tq", "run-1", 1)));
    events.add(ev(2, new Attributes.WorkflowTaskScheduled("tq", 60_000, 1)));
    events.add(ev(3, new Attributes.WorkflowTaskStarted(2, "worker", "req")));
    events.add(ev(4, new Attributes.WorkflowTaskCompleted(2, 3, "worker")));
    events.add(ev(5, new Attributes.ActivityTaskScheduled("a1", "A", "tq", 4)));
    events.add(ev(6, new Attributes.ActivityTaskStarted(5, "worker", "req", 1)));
    events.add(ev(7, new Attributes.ActivityTaskCancelRequested(5, 4)));
    events.add(ev(8, new Attributes.ActivityTaskCanceled(5, 6, 7)));
    events.add(ev(9, new Attributes.ActivityTaskScheduled("a2", "A", "tq", 4)));
    events.add(ev(10, new Attributes.ActivityTaskStarted(9, "worker", "req", 1)));
    events.add(ev(11, new Attributes.ActivityTaskCompleted(9, 10, "worker")));
    events.add(ev(12, new Attributes.ActivityTaskScheduled("a3", "A", "tq", 4)));
    events.add(ev(13, new Attributes.ActivityTaskFailed(12, 0, "worker")));
    events.add(ev(14, new Attributes.ActivityTaskScheduled("a4", "A", "tq", 4)));
    events.add(ev(15, new Attributes.ActivityTaskTimedOut(14, 0, "Timeout")));
    events.add(ev(16, new Attributes.TimerStarted("t1", 60_000, 4)));
    events.add(ev(17, new Attributes.TimerFired("t1", 16)));
    events.add(ev(18, new Attributes.TimerStarted("t2", 60_000, 4)));
    events.add(ev(19, new Attributes.TimerCanceled("t2", 18, 4)));
    events.add(ev(20, new Attributes.StartChildWorkflowExecutionInitiated("ns", "c1", "C", "tq", 4)));
    events.add(ev(21, new Attributes.ChildWorkflowExecutionStarted(20, "c1", "cr", "C")));
    events.add(ev(22, new Attributes.ChildWorkflowExecutionCompleted(20, 21, "c1")));
    events.add(ev(23, new Attributes.StartChildWorkflowExecutionInitiated("ns", "c2", "C", "tq", 4)));
    events.add(ev(24, new Attributes.ChildWorkflowExecutionFailed(23, 0, "c2", "MaximumAttemptsReached")));
    events.add(ev(25, new Attributes.StartChildWorkflowExecutionInitiated("ns", "c3", "C", "tq", 4)));
    events.add(ev(26, new Attributes.ChildWorkflowExecutionCanceled(25, 0, "c3")));
    events.add(ev(27, new Attributes.StartChildWorkflowExecutionInitiated("ns", "c4", "C", "tq", 4)));
    events.add(ev(28, new Attributes.ChildWorkflowExecutionTimedOut(27, 0, "c4", "Timeout")));
    events.add(ev(29, new Attributes.StartChildWorkflowExecutionInitiated("ns", "c5", "C", "tq", 4)));
    events.add(ev(30, new Attributes.ChildWorkflowExecutionTerminated(29, 0, "c5")));
    events.add(ev(31, new Attributes.StartChildWorkflowExecutionInitiated("ns", "c6", "C", "tq", 4)));
    events.add(ev(32, new Attributes.StartChildWorkflowExecutionFailed(31, "c6", "WorkflowAlreadyExists")));
    events.add(ev(33, new Attributes.RequestCancelExternalWorkflowExecutionInitiated("ns", "o", "or", 4)));
    events.add(ev(34, new Attributes.SignalExternalWorkflowExecutionInitiated("ns", "o", "or", "s", 4)));
    events.add(ev(35, new Attributes.WorkflowExecutionSignaled("s1", "client")));
    events.add(ev(36, new Attributes.WorkflowExecutionCancelRequested("client")));
    events.add(ev(37, new Attributes.MarkerRecorded("Version", 4)));
    events.add(ev(38, new Attributes.UpsertWorkflowSearchAttributes(Map.of("CustomKeywordField", "k"), 4)));
    events.add(ev(39, new Attributes.WorkflowTaskScheduled("tq", 60_000, 1)));
    events.add(ev(40, new Attributes.WorkflowTaskStarted(39, "worker", "req")));
    events.add(ev(41, new Attributes.WorkflowTaskFailed(39, 40, "UnhandledCommand")));
    events.add(ev(42, new Attributes.WorkflowTaskScheduled("tq", 60_000, 2)));
    events.add(ev(43, new Attributes.WorkflowTaskStarted(42, "worker", "req")));
    events.add(ev(44, new Attributes.WorkflowTaskTimedOut(42, 43, "StartToClose")));

    var state = post("wf-2", "run-1", events);

    assertThat(state.nextEventId()).isEqualTo(45L);
    assertThat(state.pendingActivities()).isEmpty();
    assertThat(state.pendingTimers()).isEmpty();
    assertThat(state.pendingChildren()).isEmpty();
    assertThat(state.pendingCancelExternal()).containsOnlyKeys(33L);
    assertThat(state.pendingSignalExternal()).containsOnlyKeys(34L);
    assertThat(state.signalCount()).isEqualTo(1L);
    assertThat(state.cancelRequested()).isTrue();
    assertThat(state.searchAttributes()).isEqualTo(Map.of("CustomKeywordField", "k"));
    // Two failures, and each start in between put the attempt back to 1 (SPEC-001 R8), so the
    // task left behind by the second is at attempt 2 rather than 3.
    assertThat(state.workflowTask().attempt()).isEqualTo(2);
    assertThat(state.workflowTask().scheduledEventId()).isEqualTo(1L);
    assertThat(get("wf-2", "run-1")).isEqualTo(state);

    // Each terminal event on its own run, so a whole-history reply cannot hide one of them.
    record Terminal(String runId, Attributes attributes, RebuiltState.Status status) {}
    for (Terminal t : List.of(
        new Terminal("t-completed", new Attributes.WorkflowExecutionCompleted(4), RebuiltState.Status.Completed),
        new Terminal("t-failed", new Attributes.WorkflowExecutionFailed(4, "boom"), RebuiltState.Status.Failed),
        new Terminal("t-timedout", new Attributes.WorkflowExecutionTimedOut("Timeout"), RebuiltState.Status.TimedOut),
        new Terminal("t-terminated", new Attributes.WorkflowExecutionTerminated("r", "c"),
            RebuiltState.Status.Terminated),
        new Terminal("t-canceled", new Attributes.WorkflowExecutionCanceled(4), RebuiltState.Status.Canceled),
        new Terminal("t-can", new Attributes.WorkflowExecutionContinuedAsNew("run-2", "T", "tq", 4),
            RebuiltState.Status.ContinuedAsNew))) {
      var s = post("wf-3", t.runId(), List.of(
          ev(1, new Attributes.WorkflowExecutionStarted("T", "tq", t.runId(), 1)),
          ev(2, t.attributes())));
      assertThat(s.status()).as(t.runId()).isEqualTo(t.status());
      assertThat(s.completionEventBatchId()).isEqualTo(1L);
    }
  }

  @Test
  public void theStructureReportIsAvailableWithoutChangingWhatReplayDecides() {
    var request = new ReplayEndpoint.HistoryRequest(List.of(new HistoryEvent.Batch(List.of(
        ev(1, new Attributes.WorkflowExecutionStarted("T", "tq", "run-1", 1)),
        ev(2, new Attributes.WorkflowTaskScheduled("tq", 60_000, 1)),
        ev(9, new Attributes.WorkflowTaskStarted(2, "worker", "req"))))));

    var report = httpClient
        .POST("/replay/structure")
        .withRequestBody(request)
        .responseBodyAs(Map.class)
        .invoke()
        .body();
    assertThat(report.get("gapsAfter")).isEqualTo(List.of(3));

    var state = httpClient
        .POST("/replay/direct/run-1")
        .withRequestBody(request)
        .responseBodyAs(RebuiltState.class)
        .invoke()
        .body();
    assertThat(state.nextEventId()).isEqualTo(10L);
  }

  @Test
  public void aRejectedBatchComesBackAsAnErrorRatherThanAState() {
    post("wf-4", "run-1", List.of(
        ev(1, new Attributes.WorkflowExecutionStarted("T", "tq", "run-1", 1)),
        ev(5, new Attributes.WorkflowTaskScheduled("tq", 60_000, 1))));
    var response = httpClient
        .POST("/replay/wf-4/run-1/batches")
        .withRequestBody(new HistoryEvent.Batch(List.of(ev(3, new Attributes.WorkflowTaskStarted(5, "w", "r")))))
        .invoke()
        .httpResponse();
    assertThat(response.status().intValue()).isGreaterThanOrEqualTo(400);
    // The state the rejected batch would have changed is untouched.
    assertThat(get("wf-4", "run-1").nextEventId()).isEqualTo(6L);
  }
}
