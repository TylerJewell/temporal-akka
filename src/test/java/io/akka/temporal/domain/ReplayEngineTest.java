package io.akka.temporal.domain;

import static io.akka.temporal.domain.Histories.activityCanceled;
import static io.akka.temporal.domain.Histories.activityCancelRequested;
import static io.akka.temporal.domain.Histories.activityCompleted;
import static io.akka.temporal.domain.Histories.activityFailed;
import static io.akka.temporal.domain.Histories.activityScheduled;
import static io.akka.temporal.domain.Histories.activityStarted;
import static io.akka.temporal.domain.Histories.activityTimedOut;
import static io.akka.temporal.domain.Histories.batch;
import static io.akka.temporal.domain.Histories.cancelExternalInitiated;
import static io.akka.temporal.domain.Histories.cancelRequested;
import static io.akka.temporal.domain.Histories.canceled;
import static io.akka.temporal.domain.Histories.childCanceled;
import static io.akka.temporal.domain.Histories.childCompleted;
import static io.akka.temporal.domain.Histories.childFailed;
import static io.akka.temporal.domain.Histories.childInitiated;
import static io.akka.temporal.domain.Histories.childStartFailed;
import static io.akka.temporal.domain.Histories.childStarted;
import static io.akka.temporal.domain.Histories.childTerminated;
import static io.akka.temporal.domain.Histories.childTimedOut;
import static io.akka.temporal.domain.Histories.completed;
import static io.akka.temporal.domain.Histories.continuedAsNew;
import static io.akka.temporal.domain.Histories.failed;
import static io.akka.temporal.domain.Histories.markerRecorded;
import static io.akka.temporal.domain.Histories.signalExternalInitiated;
import static io.akka.temporal.domain.Histories.signaled;
import static io.akka.temporal.domain.Histories.started;
import static io.akka.temporal.domain.Histories.terminated;
import static io.akka.temporal.domain.Histories.timedOut;
import static io.akka.temporal.domain.Histories.timerCanceled;
import static io.akka.temporal.domain.Histories.timerFired;
import static io.akka.temporal.domain.Histories.timerStarted;
import static io.akka.temporal.domain.Histories.unknown;
import static io.akka.temporal.domain.Histories.upsertSearchAttributes;
import static io.akka.temporal.domain.Histories.wftCompleted;
import static io.akka.temporal.domain.Histories.wftFailed;
import static io.akka.temporal.domain.Histories.wftScheduled;
import static io.akka.temporal.domain.Histories.wftScheduledAtAttempt;
import static io.akka.temporal.domain.Histories.wftStarted;
import static io.akka.temporal.domain.Histories.wftTimedOut;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.akka.temporal.domain.RebuiltState.Status;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * One test per rule in SPEC-001 §3, named as the conformance table names it. Every expected value
 * here was read off a run of the source through {@code temporal-port/probes/replay-harness}, not
 * inferred from the source's code.
 */
public class ReplayEngineTest {

  private static final String RUN = "run-1";

  /** The four events every history in the source probes started with. */
  private static List<HistoryEvent> base() {
    return List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 2), wftCompleted(4, 0, 2, 3));
  }

  private static List<HistoryEvent> baseWith(HistoryEvent... extra) {
    var all = new java.util.ArrayList<>(base());
    all.addAll(List.of(extra));
    return all;
  }

  @SafeVarargs
  private static RebuiltState replay(List<HistoryEvent>... batches) {
    return ReplayEngine.replay(RUN, java.util.Arrays.stream(batches).map(Histories::batch).toList());
  }

  // ---- batch-level rules ----

  @Test
  public void anEmptyBatchIsRejected() {
    var e = assertThrows(ReplayException.class, () -> ReplayEngine.replay(RUN, List.of(batch(List.of()))));
    assertEquals("encounter history size being zero", e.getMessage());
  }

  @Test
  public void currentVersionComesFromTheLastEventOfTheBatch() {
    var rising = replay(List.of(started(1, 0), wftScheduled(2, 0)), List.of(wftStarted(3, 7, 2)),
        List.of(wftCompleted(4, 9, 2, 3)));
    assertEquals(9L, rising.currentVersion());
    // Single-version batches cannot tell "the batch's last event" from "its first". A batch
    // holding two versions can, and the answer moves down: the update is forced, not a maximum.
    assertEquals(7L, replay(List.of(started(1, 3), wftScheduled(2, 7))).currentVersion());
    assertEquals(3L, replay(List.of(started(1, 7), wftScheduled(2, 3))).currentVersion());
  }

  @Test
  public void lastFirstEventIdIsTheFirstEventOfTheLastBatch() {
    var s = replay(List.of(started(1, 0), wftScheduled(2, 0)), List.of(wftStarted(3, 0, 2), wftCompleted(4, 0, 2, 3)));
    assertEquals(3L, s.lastFirstEventId());
    assertEquals(5L, s.nextEventId());
  }

  @Test
  public void cuttingTheSameHistoryIntoDifferentBatchesMovesOnlyLastFirstEventId() {
    var all = baseWith(activityScheduled(5, 0, "a1"), activityStarted(6, 0, 5));
    var one = replay(all);
    var three = replay(all.subList(0, 2), all.subList(2, 4), all.subList(4, 6));
    assertEquals(1L, one.lastFirstEventId());
    assertEquals(5L, three.lastFirstEventId());
    // Every other field agrees, which is what makes the one that does not a difference
    // rather than a bug.
    assertEquals(one.withPosition(0, 0, one.currentVersion(), 0, one.versionHistory()),
        three.withPosition(0, 0, three.currentVersion(), 0, three.versionHistory()));
  }

  @Test
  public void versionHistoryKeepsOneItemPerContiguousVersionRun() {
    var same = replay(List.of(started(1, 0), wftScheduled(2, 0)), List.of(wftStarted(3, 0, 2)),
        List.of(wftCompleted(4, 0, 2, 3)));
    assertEquals(List.of(new RebuiltState.VersionItem(4, 0)), same.versionHistory());
    var rising = replay(List.of(started(1, 0), wftScheduled(2, 0)), List.of(wftStarted(3, 7, 2)),
        List.of(wftCompleted(4, 9, 2, 3)));
    assertEquals(List.of(new RebuiltState.VersionItem(2, 0), new RebuiltState.VersionItem(3, 7),
        new RebuiltState.VersionItem(4, 9)), rising.versionHistory());
  }

  @Test
  public void theReplayPositionIsNotReadableDuringABatch() {
    // The synthesised task's scheduled event id is whatever nextEventId reads mid-batch.
    // It is 1 at every batch split, which is the pre-replay value and not the position.
    var all = List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 2), wftFailed(4, 0, 2, 3));
    assertEquals(1L, replay(all).workflowTask().scheduledEventId());
    assertEquals(1L, replay(all.subList(0, 3), all.subList(3, 4)).workflowTask().scheduledEventId());
    assertEquals(1L, replay(all.subList(0, 2), all.subList(2, 3), all.subList(3, 4))
        .workflowTask().scheduledEventId());
  }

  @Test
  public void nextEventIdIsTheLastEventPlusOne() {
    assertEquals(10L, replay(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(9, 0, 2))).nextEventId());
    assertEquals(3L, replay(List.of(started(5, 0), wftScheduled(2, 0))).nextEventId());
  }

  // ---- event-level rules ----

  @Test
  public void theStartedEventSetsWorkflowTypeAndTaskQueue() {
    var s = replay(List.of(started(1, 0)));
    assertEquals("T", s.workflowTypeName());
    assertEquals("tq", s.taskQueue());
    assertEquals(Status.Running, s.status());
    // The run state stays Created: scheduling the first workflow task is what moves it, and
    // no other event does. Signals, markers, timers and search-attribute upserts all leave it.
    assertEquals(RebuiltState.RunState.Created, s.runState());
    assertEquals(RebuiltState.RunState.Created, replay(List.of(started(1, 0), signaled(2, 0, "s"))).runState());
    assertEquals(RebuiltState.RunState.Created, replay(List.of(started(1, 0), markerRecorded(2, 0, "m"))).runState());
    assertEquals(RebuiltState.RunState.Created, replay(List.of(started(1, 0), timerStarted(2, 0, "t"))).runState());
    assertEquals(RebuiltState.RunState.Running, replay(List.of(started(1, 0), wftScheduled(2, 0))).runState());
  }

  @Test
  public void aCompletedWorkflowTaskRecordsTheWorkersBuildIdOnce() {
    var once = replay(base());
    assertEquals(Map.of("BuildIds", "[\"unversioned\"]"), once.searchAttributes());
    var twice = replay(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 2), wftCompleted(4, 0, 2, 3),
        wftScheduled(5, 0), wftStarted(6, 0, 5), wftCompleted(7, 0, 5, 6)));
    assertEquals(Map.of("BuildIds", "[\"unversioned\"]"), twice.searchAttributes());
  }

  @Test
  public void aWorkflowTaskIsScheduledStartedAndCompleted() {
    var scheduled = replay(List.of(started(1, 0), wftScheduled(2, 0)));
    assertEquals(2L, scheduled.workflowTask().scheduledEventId());
    assertEquals(0L, scheduled.workflowTask().startedEventId());
    assertEquals(1, scheduled.workflowTask().attempt());
    var startedTask = replay(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 2)));
    assertEquals(3L, startedTask.workflowTask().startedEventId());
    var done = replay(base());
    assertNull(done.workflowTask());
    assertEquals(3L, done.lastCompletedWorkflowTaskStartedEventId());
  }

  @Test
  public void aFailedWorkflowTaskLeavesASynthesisedOne() {
    var s = replay(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 2), wftFailed(4, 0, 2, 3)));
    assertNotNull(s.workflowTask());
    assertEquals(1L, s.workflowTask().scheduledEventId());
    assertEquals(0L, s.workflowTask().startedEventId());
    assertEquals(2, s.workflowTask().attempt());
    assertEquals(0L, s.lastCompletedWorkflowTaskStartedEventId());
  }

  @Test
  public void aTimedOutWorkflowTaskLeavesASynthesisedOne() {
    var s = replay(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 2),
        wftTimedOut(4, 0, 2, 3, "StartToClose")));
    assertNotNull(s.workflowTask());
    assertEquals(1L, s.workflowTask().scheduledEventId());
    assertEquals(0L, s.workflowTask().startedEventId());
    assertEquals(2, s.workflowTask().attempt());
  }

  @Test
  public void aScheduleToStartTimeoutDoesNotSynthesiseATask() {
    var s = replay(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 2),
        wftTimedOut(4, 0, 2, 3, "ScheduleToStart")));
    assertNull(s.workflowTask());
  }

  @Test
  public void aSecondScheduleOnTheSameEventIdOverwritesTheFirst() {
    var s = replay(baseWith(activityScheduled(5, 0, "a1"), activityScheduled(5, 0, "a2")));
    assertEquals(1, s.pendingActivities().size());
    assertEquals("a2", s.pendingActivities().get(5L).activityId());
  }

  @Test
  public void everyActivityTerminalEventRemovesThePendingActivity() {
    var scheduled = replay(baseWith(activityScheduled(5, 0, "a1")));
    assertEquals(0L, scheduled.pendingActivities().get(5L).startedEventId());
    assertEquals(1, scheduled.pendingActivities().get(5L).attempt());
    var startedActivity = replay(baseWith(activityScheduled(5, 0, "a1"), activityStarted(6, 0, 5)));
    assertEquals(6L, startedActivity.pendingActivities().get(5L).startedEventId());
    for (HistoryEvent terminal : List.of(activityCompleted(7, 0, 5, 6), activityFailed(7, 0, 5, 6),
        activityTimedOut(7, 0, 5, 6), activityCanceled(7, 0, 5, 6))) {
      var s = replay(baseWith(activityScheduled(5, 0, "a1"), activityStarted(6, 0, 5), terminal));
      assertTrue(s.pendingActivities().isEmpty(), terminal.typeName() + " left a pending activity");
    }
  }

  @Test
  public void aCancelRequestMarksTheActivityAndKeepsIt() {
    var s = replay(baseWith(activityScheduled(5, 0, "a1"), activityStarted(6, 0, 5), activityCancelRequested(7, 0, 5)));
    assertTrue(s.pendingActivities().get(5L).cancelRequested());
    assertEquals(6L, s.pendingActivities().get(5L).startedEventId());
  }

  @Test
  public void aTimerIsStartedThenFiredOrCancelled() {
    var startedTimer = replay(baseWith(timerStarted(5, 0, "t1")));
    assertEquals(5L, startedTimer.pendingTimers().get("t1").startedEventId());
    assertTrue(replay(baseWith(timerStarted(5, 0, "t1"), timerFired(6, 0, "t1", 5))).pendingTimers().isEmpty());
    assertTrue(replay(baseWith(timerStarted(5, 0, "t1"), timerCanceled(6, 0, "t1", 5))).pendingTimers().isEmpty());
  }

  @Test
  public void everyChildTerminalEventRemovesThePendingChild() {
    var initiated = replay(baseWith(childInitiated(5, 0, "c1")));
    assertEquals(0L, initiated.pendingChildren().get(5L).startedEventId());
    // The workflow id is known from the initiated event, before anything has started.
    assertEquals("c1", initiated.pendingChildren().get(5L).workflowId());
    var startedChild = replay(baseWith(childInitiated(5, 0, "c1"), childStarted(6, 0, 5, "c1", "cr")));
    assertEquals(6L, startedChild.pendingChildren().get(5L).startedEventId());
    assertEquals("c1", startedChild.pendingChildren().get(5L).workflowId());
    for (HistoryEvent terminal : List.of(childCompleted(7, 0, 5, 6, "c1"), childFailed(7, 0, 5, 6, "c1"),
        childCanceled(7, 0, 5, 6, "c1"), childTimedOut(7, 0, 5, 6, "c1"), childTerminated(7, 0, 5, 6, "c1"))) {
      var s = replay(baseWith(childInitiated(5, 0, "c1"), childStarted(6, 0, 5, "c1", "cr"), terminal));
      assertTrue(s.pendingChildren().isEmpty(), terminal.typeName() + " left a pending child");
    }
    assertTrue(replay(baseWith(childInitiated(5, 0, "c1"), childStartFailed(6, 0, 5, "c1")))
        .pendingChildren().isEmpty());
  }

  @Test
  public void anEventNamingSomethingNotPendingIsIgnored() {
    var timer = replay(baseWith(timerFired(5, 0, "nope", 4)));
    assertTrue(timer.pendingTimers().isEmpty());
    assertEquals(6L, timer.nextEventId());
    var activity = replay(baseWith(activityCompleted(5, 0, 99, 100)));
    assertTrue(activity.pendingActivities().isEmpty());
    assertEquals(6L, activity.nextEventId());
  }

  @Test
  public void signalsAreCountedAndNotPending() {
    var s = replay(List.of(started(1, 0), wftScheduled(2, 0), signaled(3, 0, "s1"), signaled(4, 0, "s2")));
    assertEquals(2L, s.signalCount());
    assertEquals(5L, s.nextEventId());
  }

  @Test
  public void aCancelRequestDoesNotChangeStatus() {
    var s = replay(List.of(started(1, 0), wftScheduled(2, 0), cancelRequested(3, 0)));
    assertTrue(s.cancelRequested());
    assertEquals(Status.Running, s.status());
  }

  @Test
  public void everyTerminalEventRecordsTheBatchIdNotItsOwnId() {
    record Case(HistoryEvent event, Status status) {}
    for (Case c : List.of(new Case(completed(5, 0, 4), Status.Completed), new Case(failed(5, 0, 4), Status.Failed),
        new Case(timedOut(5, 0), Status.TimedOut), new Case(terminated(5, 0), Status.Terminated),
        new Case(canceled(5, 0, 4), Status.Canceled))) {
      var s = replay(baseWith(c.event()));
      assertEquals(c.status(), s.status());
      assertEquals(RebuiltState.RunState.Completed, s.runState());
      assertEquals(1L, s.completionEventBatchId(), c.event().typeName() + " recorded its own id");
    }
  }

  @Test
  public void continueAsNewProducesASecondState() {
    var result = ReplayEngine.replay(RUN, List.of(batch(baseWith(continuedAsNew(5, 0, 4, "run-2")))),
        List.of(started(1, 0), wftScheduled(2, 0)), "run-2");
    assertEquals(Status.ContinuedAsNew, result.state().status());
    assertEquals(RebuiltState.RunState.Completed, result.state().runState());
    assertNotNull(result.newRunState());
    assertEquals(Status.Running, result.newRunState().status());
    assertEquals(3L, result.newRunState().nextEventId());
    assertEquals(2L, result.newRunState().workflowTask().scheduledEventId());

    var alone = ReplayEngine.replay(RUN, List.of(batch(baseWith(continuedAsNew(5, 0, 4, "run-2")))), List.of(), "");
    assertNull(alone.newRunState());
    assertEquals(Status.ContinuedAsNew, alone.state().status());
  }

  @Test
  public void aNewRunHistoryWhileStillRunningIsRejected() {
    var e = assertThrows(ReplayException.class,
        () -> ReplayEngine.replay(RUN, List.of(batch(base())), List.of(started(1, 0)), "run-2"));
    assertEquals("Cannot apply events for new run when current run is still running", e.getMessage());
  }

  @Test
  public void aMarkerIsInertAndSearchAttributesAreReplaced() {
    var marker = replay(baseWith(markerRecorded(5, 0, "Version")));
    assertEquals(6L, marker.nextEventId());
    assertEquals(Status.Running, marker.status());
    // Only what the completed workflow task in the base history wrote.
    assertEquals(Map.of("BuildIds", "[\"unversioned\"]"), marker.searchAttributes());
    var upsert = replay(baseWith(upsertSearchAttributes(5, 0, Map.of("CustomKeywordField", "k"))));
    assertEquals(Map.of("CustomKeywordField", "k"), upsert.searchAttributes());
    assertEquals(6L, upsert.nextEventId());
  }

  @Test
  public void anUnknownEventTypeIsRejected() {
    var e = assertThrows(ReplayException.class, () -> replay(baseWith(unknown(5, 0, "9999"))));
    assertEquals("Unknown event type: 9999", e.getMessage());
  }

  @Test
  public void eventsAfterATerminalEventAreStillApplied() {
    var s = replay(baseWith(completed(5, 0, 4), signaled(6, 0, "after")));
    assertEquals(Status.Completed, s.status());
    assertEquals(7L, s.nextEventId());
    assertEquals(1L, s.signalCount());
  }

  @Test
  public void aHistoryNeedNotBeginWithTheStartedEvent() {
    var s = replay(List.of(wftScheduled(1, 0), wftStarted(2, 0, 1)));
    assertEquals(Status.Running, s.status());
    assertEquals("", s.workflowTypeName());
    assertEquals(3L, s.nextEventId());
    assertNotNull(s.workflowTask());
  }

  @Test
  public void eventIdsAreNotValidatedForContiguityOrIncrease() {
    var gap = replay(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(9, 0, 2)));
    assertEquals(10L, gap.nextEventId());
    assertEquals(9L, gap.workflowTask().startedEventId());
    var descending = replay(List.of(started(5, 0), wftScheduled(2, 0)));
    assertEquals(3L, descending.nextEventId());
    assertEquals(List.of(new RebuiltState.VersionItem(2, 0)), descending.versionHistory());
  }

  @Test
  public void aLowerVersionInALaterBatchIsRejected() {
    var e = assertThrows(ReplayException.class,
        () -> replay(List.of(started(1, 5), wftScheduled(2, 5)), List.of(wftStarted(3, 3, 2))));
    assertEquals("cannot update version history with a lower version 3. Last version: 5", e.getMessage());
  }

  @Test
  public void aNonIncreasingEventIdInALaterBatchIsRejected() {
    var lower = assertThrows(ReplayException.class,
        () -> replay(List.of(started(1, 0), wftScheduled(5, 0)), List.of(wftStarted(3, 0, 5))));
    assertEquals("cannot add version history with a lower event id 3. Last event id: 5", lower.getMessage());
    var equal = assertThrows(ReplayException.class,
        () -> replay(List.of(started(1, 0), wftScheduled(5, 0)), List.of(wftStarted(5, 0, 5))));
    assertEquals("cannot add version history with a lower event id 5. Last event id: 5", equal.getMessage());
  }

  // ---- the rule the source does not satisfy ----

  @Test
  public void replayingTheSameHistoryTwiceGivesTheSameState() {
    var history = baseWith(cancelExternalInitiated(5, 0, "other"), signalExternalInitiated(6, 0, "other"));
    assertEquals(replay(history), replay(history));
  }

  @Test
  public void externalRequestIdsAreDerivedFromTheInitiatingEvent() {
    var s = replay(baseWith(cancelExternalInitiated(5, 0, "other"), signalExternalInitiated(6, 0, "other")));
    var cancel = s.pendingCancelExternal().get(5L);
    var signal = s.pendingSignalExternal().get(6L);
    assertNotNull(cancel);
    assertNotNull(signal);
    assertEquals(ReplayEngine.derivedRequestId(RUN, 5L), cancel.requestId());
    assertEquals(ReplayEngine.derivedRequestId(RUN, 6L), signal.requestId());
    // Two runs of the same workflow are different rebuilds, so the ids differ between them
    // and are stable within one.
    assertFalse(cancel.requestId().equals(ReplayEngine.derivedRequestId("run-2", 5L)));
  }

  @Test
  public void theThreeStartedEventsRefuseAReferenceToNothing() {
    var activity = assertThrows(ReplayException.class, () -> replay(baseWith(activityStarted(5, 0, 99))));
    assertEquals("unable to get activity info", activity.getMessage());
    var child = assertThrows(ReplayException.class,
        () -> replay(baseWith(childStarted(5, 0, 99, "c", "cr"))));
    assertEquals("unable to get child workflow info", child.getMessage());
    var task = assertThrows(ReplayException.class, () -> replay(baseWith(wftStarted(5, 0, 99))));
    assertEquals("unable to find workflow task: 99", task.getMessage());
    // A workflow task pending under a different scheduled id is refused the same way as none.
    var mismatched = assertThrows(ReplayException.class,
        () -> replay(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 99))));
    assertEquals("unable to find workflow task: 99", mismatched.getMessage());
  }

  @Test
  public void startingAWorkflowTaskResetsItsAttempt() {
    var scheduledAtTwo = List.of(started(1, 0), wftScheduled(2, 0), wftStarted(3, 0, 2),
        wftFailed(4, 0, 2, 3), wftScheduledAtAttempt(5, 0, 2));
    assertEquals(2, replay(scheduledAtTwo).workflowTask().attempt());
    var startedTask = new java.util.ArrayList<>(scheduledAtTwo);
    startedTask.add(wftStarted(6, 0, 5));
    assertEquals(1, replay(startedTask).workflowTask().attempt());
  }
}
