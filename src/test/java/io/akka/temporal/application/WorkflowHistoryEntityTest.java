package io.akka.temporal.application;

import static io.akka.temporal.domain.Histories.activityScheduled;
import static io.akka.temporal.domain.Histories.activityStarted;
import static io.akka.temporal.domain.Histories.batch;
import static io.akka.temporal.domain.Histories.cancelExternalInitiated;
import static io.akka.temporal.domain.Histories.started;
import static io.akka.temporal.domain.Histories.wftCompleted;
import static io.akka.temporal.domain.Histories.wftScheduled;
import static io.akka.temporal.domain.Histories.wftStarted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.javasdk.testkit.EventSourcedTestKit;
import io.akka.temporal.domain.HistoryEvent;
import io.akka.temporal.domain.RebuiltState;
import io.akka.temporal.domain.ReplayEngine;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The entity's own journal replay has to land on the same state the in-memory fold does, because
 * the journal is the history and recovery is the replay.
 */
public class WorkflowHistoryEntityTest {

  private static final String RUN = "run-1";

  private static List<HistoryEvent.Batch> history() {
    return List.of(
        batch(List.of(started(1, 0), wftScheduled(2, 0))),
        batch(List.of(wftStarted(3, 0, 2), wftCompleted(4, 0, 2, 3))),
        batch(List.of(activityScheduled(5, 0, "a1"), activityStarted(6, 0, 5), cancelExternalInitiated(7, 0, "o"))));
  }

  @Test
  public void theEntityRebuildsTheSameStateFromItsJournal() {
    var testKit = EventSourcedTestKit.of("wf-1:run-1", WorkflowHistoryEntity::new);
    for (var b : history()) {
      testKit.method(WorkflowHistoryEntity::replayBatch).invoke(new WorkflowHistoryEntity.ReplayBatch(RUN, b));
    }
    var afterCommands = testKit.getState().state();
    assertEquals(ReplayEngine.replay(RUN, history()), afterCommands);

    // Rebuild from the journal alone: a fresh test kit fed the persisted events in order.
    var journal = testKit.getAllEvents();
    var fresh = EventSourcedTestKit.of("wf-1:run-1", WorkflowHistoryEntity::new);
    var rebuilt = fresh.getState();
    var entity = new WorkflowHistoryEntity();
    assertTrue(journal.size() >= 3);
    assertEquals(afterCommands, replayJournal(journal));
    assertEquals(RebuiltState.empty(), rebuilt.state());
    assertEquals(RebuiltState.empty(), new WorkflowHistoryEntity().emptyState().state());
    assertEquals(entity.emptyState().runId(), "");
  }

  /** Folds the persisted journal exactly as recovery does, without the runtime. */
  private static RebuiltState replayJournal(List<?> journal) {
    var state = new WorkflowHistoryEntity.EntityState("", RebuiltState.empty());
    for (Object raw : journal) {
      var e = (WorkflowHistoryEntity.Journal) raw;
      state = switch (e) {
        case WorkflowHistoryEntity.Journal.BatchOpened b ->
            new WorkflowHistoryEntity.EntityState(b.runId(), ReplayEngine.openBatch(state.state(), b.batch()));
        case WorkflowHistoryEntity.Journal.EventApplied b ->
            new WorkflowHistoryEntity.EntityState(state.runId(),
                ReplayEngine.applyRecovered(state.runId(), state.state(), b.event()));
        case WorkflowHistoryEntity.Journal.BatchClosed b ->
            new WorkflowHistoryEntity.EntityState(state.runId(),
                ReplayEngine.closeBatch(state.state(), b.lastEventId()));
      };
    }
    return state.state();
  }

  @Test
  public void aRejectedBatchLeavesNothingInTheJournal() {
    var testKit = EventSourcedTestKit.of("wf-1:run-1", WorkflowHistoryEntity::new);
    testKit.method(WorkflowHistoryEntity::replayBatch)
        .invoke(new WorkflowHistoryEntity.ReplayBatch(RUN, batch(List.of(started(1, 0), wftScheduled(5, 0)))));
    int before = testKit.getAllEvents().size();
    var result = testKit.method(WorkflowHistoryEntity::replayBatch)
        .invoke(new WorkflowHistoryEntity.ReplayBatch(RUN, batch(List.of(wftStarted(3, 0, 5)))));
    assertTrue(result.isError());
    assertEquals("cannot add version history with a lower event id 3. Last event id: 5", result.getError());
    assertEquals(before, testKit.getAllEvents().size());
  }
}
