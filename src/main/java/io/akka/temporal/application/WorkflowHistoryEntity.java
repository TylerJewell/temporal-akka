package io.akka.temporal.application;

import akka.javasdk.annotations.Component;
import akka.javasdk.annotations.TypeName;
import akka.javasdk.eventsourcedentity.EventSourcedEntity;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.akka.temporal.domain.HistoryEvent;
import io.akka.temporal.domain.RebuiltState;
import io.akka.temporal.domain.ReplayEngine;
import io.akka.temporal.domain.ReplayException;

/**
 * One workflow run's history, replayed.
 *
 * <p>The journal holds the history itself, so the entity's own recovery <em>is</em> the replay:
 * the same {@link ReplayEngine} calls run whether a batch has just arrived or the runtime is
 * rebuilding the state from disk.
 *
 * <p>A batch's boundaries are journal entries of their own because SPEC-001's batch-level rules
 * (R1–R6) are decided per batch and would otherwise be lost the moment the state was rebuilt from
 * events alone — {@code lastFirstEventId} in particular is a fact about the batching, not about
 * any one event (question-log row 3).
 */
@Component(id = "workflow-history")
public class WorkflowHistoryEntity
    extends EventSourcedEntity<WorkflowHistoryEntity.EntityState, WorkflowHistoryEntity.Journal> {

  /** The run id is carried alongside the state because the derived request ids depend on it (D1). */
  public record EntityState(String runId, RebuiltState state) {}

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "journal")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = Journal.BatchOpened.class, name = "batch-opened"),
    @JsonSubTypes.Type(value = Journal.EventApplied.class, name = "event"),
    @JsonSubTypes.Type(value = Journal.BatchClosed.class, name = "batch-closed")
  })
  public sealed interface Journal {

    @TypeName("batch-opened")
    record BatchOpened(String runId, HistoryEvent.Batch batch) implements Journal {}

    @TypeName("event")
    record EventApplied(HistoryEvent event) implements Journal {}

    @TypeName("batch-closed")
    record BatchClosed(long lastEventId) implements Journal {}
  }

  public record ReplayBatch(String runId, HistoryEvent.Batch batch) {}

  @Override
  public EntityState emptyState() {
    return new EntityState("", RebuiltState.empty());
  }

  /**
   * Replays one persistence batch, the same unit the source's caller hands over one call at a time
   * (question-log row 24).
   *
   * <p>The batch is validated against R1–R6 and R20 before anything is persisted, so a history the
   * engine rejects leaves no journal entries behind — a rejected replay and a replay that never
   * happened are the same state.
   */
  public Effect<RebuiltState> replayBatch(ReplayBatch cmd) {
    RebuiltState projected;
    try {
      projected = ReplayEngine.replayBatch(cmd.runId(), currentState().state(), cmd.batch());
    } catch (ReplayException e) {
      return effects().error(e.getMessage());
    }
    var journal = new java.util.ArrayList<Journal>(cmd.batch().events().size() + 2);
    journal.add(new Journal.BatchOpened(cmd.runId(), cmd.batch()));
    for (var event : cmd.batch().events()) {
      journal.add(new Journal.EventApplied(event));
    }
    journal.add(new Journal.BatchClosed(cmd.batch().events().get(cmd.batch().events().size() - 1).eventId()));
    return effects().persistAll(journal).thenReply(s -> projected);
  }

  public ReadOnlyEffect<RebuiltState> read() {
    return effects().reply(currentState().state());
  }

  /**
   * Total by construction: every reason a batch can be refused is decided in
   * {@link ReplayEngine#replayBatch}, which runs before anything is persisted, so nothing reached
   * from here can throw. An event handler that throws leaves the entity unable to load at all.
   */
  @Override
  public EntityState applyEvent(Journal journal) {
    var current = currentState();
    return switch (journal) {
      case Journal.BatchOpened e -> new EntityState(e.runId(), ReplayEngine.openBatch(current.state(), e.batch()));
      case Journal.EventApplied e -> new EntityState(current.runId(),
          ReplayEngine.applyRecovered(current.runId(), current.state(), e.event()));
      case Journal.BatchClosed e ->
          new EntityState(current.runId(), ReplayEngine.closeBatch(current.state(), e.lastEventId()));
    };
  }
}
