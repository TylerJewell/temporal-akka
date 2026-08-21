package io.akka.temporal.domain;

import io.akka.temporal.domain.HistoryEvent.Attributes;
import io.akka.temporal.domain.RebuiltState.PendingActivity;
import io.akka.temporal.domain.RebuiltState.PendingChild;
import io.akka.temporal.domain.RebuiltState.PendingExternal;
import io.akka.temporal.domain.RebuiltState.PendingTimer;
import io.akka.temporal.domain.RebuiltState.Status;
import io.akka.temporal.domain.RebuiltState.VersionItem;
import io.akka.temporal.domain.RebuiltState.WorkflowTask;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The decision procedure of SPEC-001: a persisted history folds into a rebuilt state.
 *
 * <p>Split into {@link #openBatch}, {@link #apply} and {@link #closeBatch} rather than one loop,
 * because the entity replays its journal one event at a time and the batch boundaries have to be
 * events in that journal for the batch-level rules to survive a reload. {@link #replay} is the same
 * three steps run in memory.
 */
public final class ReplayEngine {

  private ReplayEngine() {}

  /**
   * The search attribute a completed workflow task writes, and the one value in scope. Worker
   * versioning is out of scope (SPEC-001 §1), and an unversioned worker is what the source records
   * when no versioning stamp accompanies the completion.
   */
  private static final String BUILD_IDS = "BuildIds";

  private static final String UNVERSIONED = "[\"unversioned\"]";

  private static Map<String, String> withBuildIds(Map<String, String> attributes) {
    var next = new TreeMap<>(attributes);
    next.put(BUILD_IDS, UNVERSIONED);
    return next;
  }

  /** A rebuild, and the second one a continue-as-new hands off to. */
  public record Result(RebuiltState state, RebuiltState newRunState) {}

  public static RebuiltState replay(String runId, List<HistoryEvent.Batch> history) {
    return replay(runId, history, List.of(), "").state();
  }

  public static Result replay(String runId, List<HistoryEvent.Batch> history,
                              List<HistoryEvent> newRunHistory, String newRunId) {
    var state = RebuiltState.empty();
    for (var b : history) {
      state = replayBatch(runId, state, b);
    }
    if (newRunHistory.isEmpty()) {
      return new Result(state, null);
    }
    if (state.status() == Status.Running) {
      throw new ReplayException("Cannot apply events for new run when current run is still running");
    }
    var newRun = replayBatch(newRunId, RebuiltState.empty(), new HistoryEvent.Batch(newRunHistory));
    return new Result(state, newRun);
  }

  /**
   * One batch, and every reason a batch can be refused.
   *
   * <p>This is the strict path: R1, R20, R26 and R15b are all decided here, so a caller that gets
   * a state back has a batch nothing can refuse. {@link #openBatch}, {@link #applyRecovered} and
   * {@link #closeBatch} are the same fold with the refusals taken out, for replaying a journal
   * that already came through here — an event handler that can throw is an entity that can fail
   * to load.
   */
  public static RebuiltState replayBatch(String runId, RebuiltState state, HistoryEvent.Batch batch) {
    if (batch.events().isEmpty()) {
      throw new ReplayException("encounter history size being zero");
    }
    var last = batch.events().get(batch.events().size() - 1);
    checkVersionHistory(state.versionHistory(), new VersionItem(last.eventId(), last.version()));
    var next = openBatch(state, batch);
    for (var event : batch.events()) {
      next = apply(runId, next, event);
    }
    return closeBatch(next, last.eventId());
  }

  /**
   * R2–R5. Everything a batch decides before its first event is applied, in the source's order:
   * take the current version from the batch's last event, record the batch's first event id,
   * extend the version history, and put the readable position back to where the store left it.
   *
   * <p>Total: an empty batch is refused by {@link #replayBatch} and cannot arrive here.
   */
  public static RebuiltState openBatch(RebuiltState state, HistoryEvent.Batch batch) {
    if (batch.events().isEmpty()) {
      return state;
    }
    var first = batch.events().get(0);
    var last = batch.events().get(batch.events().size() - 1);
    return state.withPosition(
        // R5: the readable next event id during the batch is the store's, which for a rebuild
        // from scratch is the first event id and never advances.
        RebuiltState.empty().nextEventId(),
        first.eventId(),
        last.version(),
        first.eventId(),
        addOrUpdateVersionHistory(state.versionHistory(), new VersionItem(last.eventId(), last.version())));
  }

  /** R6. The position after a batch is its last event's id plus one — not a count, not a maximum. */
  public static RebuiltState closeBatch(RebuiltState state, long lastEventId) {
    return state.withNextEventId(lastEventId + 1);
  }

  /** R26. The only validation replay performs, and the only one that looks across batches. */
  private static void checkVersionHistory(List<VersionItem> items, VersionItem item) {
    if (items.isEmpty()) {
      return;
    }
    var lastItem = items.get(items.size() - 1);
    if (item.version() < lastItem.version()) {
      throw new ReplayException("cannot update version history with a lower version " + item.version()
          + ". Last version: " + lastItem.version());
    }
    if (item.eventId() <= lastItem.eventId()) {
      throw new ReplayException("cannot add version history with a lower event id " + item.eventId()
          + ". Last event id: " + lastItem.eventId());
    }
  }

  /** R4. Total: {@link #checkVersionHistory} has already refused the pairs this cannot represent. */
  private static List<VersionItem> addOrUpdateVersionHistory(List<VersionItem> items, VersionItem item) {
    if (items.isEmpty()) {
      return List.of(item);
    }
    var lastItem = items.get(items.size() - 1);
    var next = new ArrayList<>(items);
    if (item.version() > lastItem.version()) {
      next.add(item);
    } else {
      next.set(next.size() - 1, new VersionItem(item.eventId(), lastItem.version()));
    }
    return List.copyOf(next);
  }

  /** R7–R24, strict: the three events of R15b refuse a reference to something not pending. */
  public static RebuiltState apply(String runId, RebuiltState state, HistoryEvent event) {
    return apply(runId, state, event, true);
  }

  /**
   * R7–R24, total: the same fold with nothing left to refuse, for replaying a journal whose
   * batches already went through {@link #replayBatch}.
   */
  public static RebuiltState applyRecovered(String runId, RebuiltState state, HistoryEvent event) {
    return apply(runId, state, event, false);
  }

  private static RebuiltState apply(String runId, RebuiltState state, HistoryEvent event, boolean strict) {
    return switch (event.attributes()) {
      // R7: the started event sets the type and the task queue and leaves the run state at
      // Created. Scheduling the first workflow task is what moves it to Running.
      case Attributes.WorkflowExecutionStarted a -> state.withStart(a.workflowType(), a.taskQueue());

      case Attributes.WorkflowTaskScheduled a ->
          state.with(state.status(), RebuiltState.RunState.Running)
              .withWorkflowTask(new WorkflowTask(event.eventId(), 0L, a.attempt(), a.taskQueue(), ""),
                  state.lastCompletedWorkflowTaskStartedEventId());

      // R15b, and the attempt reset in R8: starting a workflow task puts its attempt back to 1,
      // so a task that became transient through an earlier failure stops being transient.
      case Attributes.WorkflowTaskStarted a -> {
        var task = state.workflowTask();
        if (task == null || task.scheduledEventId() != a.scheduledEventId()) {
          if (strict) {
            throw new ReplayException("unable to find workflow task: " + a.scheduledEventId());
          }
          yield state;
        }
        yield state.withWorkflowTask(
            new WorkflowTask(task.scheduledEventId(), event.eventId(), 1, task.taskQueue(), a.requestId()),
            state.lastCompletedWorkflowTaskStartedEventId());
      }

      // R19b: completing a workflow task records the worker's build id in the search
      // attributes, once. An unversioned worker is the only case in scope, and repeats do not
      // accumulate.
      case Attributes.WorkflowTaskCompleted a -> {
        var next = state.withWorkflowTask(null, a.startedEventId());
        yield state.searchAttributes().containsKey(BUILD_IDS) ? next
            : next.withSearchAttributes(withBuildIds(state.searchAttributes()));
      }

      case Attributes.WorkflowTaskFailed ignored -> failWorkflowTask(state, true);

      // R21: schedule-to-start is the one timeout that does not charge an attempt, so no
      // synthesised task follows it.
      case Attributes.WorkflowTaskTimedOut a -> failWorkflowTask(state, !"ScheduleToStart".equals(a.timeoutType()));

      case Attributes.ActivityTaskScheduled a -> state.withActivities(
          RebuiltState.put(state.pendingActivities(), event.eventId(),
              new PendingActivity(event.eventId(), 0L, a.activityId(), a.activityType(), 1, false)));

      // R15b: the one activity event that requires the activity to be there.
      case Attributes.ActivityTaskStarted a -> {
        if (strict && !state.pendingActivities().containsKey(a.scheduledEventId())) {
          throw new ReplayException("unable to get activity info");
        }
        yield mapActivity(state, a.scheduledEventId(),
            p -> new PendingActivity(p.scheduledEventId(), event.eventId(), p.activityId(), p.activityType(),
                a.attempt(), p.cancelRequested()));
      }

      case Attributes.ActivityTaskCancelRequested a -> mapActivity(state, a.scheduledEventId(),
          p -> new PendingActivity(p.scheduledEventId(), p.startedEventId(), p.activityId(), p.activityType(),
              p.attempt(), true));

      case Attributes.ActivityTaskCompleted a -> removeActivity(state, a.scheduledEventId());
      case Attributes.ActivityTaskFailed a -> removeActivity(state, a.scheduledEventId());
      case Attributes.ActivityTaskTimedOut a -> removeActivity(state, a.scheduledEventId());
      case Attributes.ActivityTaskCanceled a -> removeActivity(state, a.scheduledEventId());

      case Attributes.TimerStarted a -> state.withTimers(
          RebuiltState.putTimer(state.pendingTimers(), a.timerId(), new PendingTimer(a.timerId(), event.eventId())));
      case Attributes.TimerFired a -> state.withTimers(RebuiltState.removeTimer(state.pendingTimers(), a.timerId()));
      case Attributes.TimerCanceled a -> state.withTimers(RebuiltState.removeTimer(state.pendingTimers(), a.timerId()));

      // R13: the child's workflow id is known at initiation, not at start; the started event
      // fills in the started event id and nothing else.
      case Attributes.StartChildWorkflowExecutionInitiated a -> state.withChildren(
          RebuiltState.put(state.pendingChildren(), event.eventId(),
              new PendingChild(event.eventId(), 0L, a.workflowId(), a.workflowType())));

      // R15b: the one child event that requires the child to be there.
      case Attributes.ChildWorkflowExecutionStarted a -> {
        if (strict && !state.pendingChildren().containsKey(a.initiatedEventId())) {
          throw new ReplayException("unable to get child workflow info");
        }
        yield mapChild(state, a.initiatedEventId(),
            p -> new PendingChild(p.initiatedEventId(), event.eventId(), p.workflowId(), p.workflowTypeName()));
      }

      case Attributes.StartChildWorkflowExecutionFailed a -> removeChild(state, a.initiatedEventId());
      case Attributes.ChildWorkflowExecutionCompleted a -> removeChild(state, a.initiatedEventId());
      case Attributes.ChildWorkflowExecutionFailed a -> removeChild(state, a.initiatedEventId());
      case Attributes.ChildWorkflowExecutionCanceled a -> removeChild(state, a.initiatedEventId());
      case Attributes.ChildWorkflowExecutionTimedOut a -> removeChild(state, a.initiatedEventId());
      case Attributes.ChildWorkflowExecutionTerminated a -> removeChild(state, a.initiatedEventId());

      case Attributes.RequestCancelExternalWorkflowExecutionInitiated a -> state.withExternals(
          RebuiltState.put(state.pendingCancelExternal(), event.eventId(),
              new PendingExternal(event.eventId(), a.workflowId(), a.runId(), derivedRequestId(runId, event.eventId()))),
          state.pendingSignalExternal());

      case Attributes.SignalExternalWorkflowExecutionInitiated a -> state.withExternals(
          state.pendingCancelExternal(),
          RebuiltState.put(state.pendingSignalExternal(), event.eventId(),
              new PendingExternal(event.eventId(), a.workflowId(), a.runId(), derivedRequestId(runId, event.eventId()))));

      case Attributes.WorkflowExecutionSignaled ignored -> state.withSignalCount(state.signalCount() + 1);
      case Attributes.WorkflowExecutionCancelRequested ignored -> state.withCancelRequested(true);

      // R19: recorded in history, inert in state.
      case Attributes.MarkerRecorded ignored -> state;
      case Attributes.UpsertWorkflowSearchAttributes a ->
          state.withSearchAttributes(Map.copyOf(a.indexedFields()));

      case Attributes.WorkflowExecutionCompleted ignored -> complete(state, Status.Completed);
      case Attributes.WorkflowExecutionFailed ignored -> complete(state, Status.Failed);
      case Attributes.WorkflowExecutionTimedOut ignored -> complete(state, Status.TimedOut);
      case Attributes.WorkflowExecutionTerminated ignored -> complete(state, Status.Terminated);
      case Attributes.WorkflowExecutionCanceled ignored -> complete(state, Status.Canceled);
      case Attributes.WorkflowExecutionContinuedAsNew ignored -> complete(state, Status.ContinuedAsNew);

      case Attributes.Unknown a -> {
        if (strict) {
          throw new ReplayException("Unknown event type: " + a.type());
        }
        yield state;
      }
    };
  }

  /**
   * R9. The scheduled event id of the synthesised task is whatever the position reads mid-batch,
   * which R5 pins to the store's value. The source's own comment calls the value wrong; it is
   * reproduced rather than corrected, so that a rebuilt state can be compared field for field
   * (SPEC-001 D2).
   */
  private static RebuiltState failWorkflowTask(RebuiltState state, boolean incrementAttempt) {
    var task = state.workflowTask();
    int attempt = incrementAttempt ? (task == null ? 1 : task.attempt()) + 1 : 1;
    if (attempt <= 1) {
      return state.withWorkflowTask(null, state.lastCompletedWorkflowTaskStartedEventId());
    }
    return state.withWorkflowTask(
        new WorkflowTask(state.nextEventId(), 0L, attempt, task == null ? "" : task.taskQueue(), ""),
        state.lastCompletedWorkflowTaskStartedEventId());
  }

  /** R17. The batch's first event id is what is recorded, not the terminal event's own id. */
  private static RebuiltState complete(RebuiltState state, Status status) {
    return state.withCompletion(status, state.replayEventBatchId());
  }

  /** R14, for activities: an id nobody scheduled is not an error and not a change. */
  private static RebuiltState mapActivity(RebuiltState state, long scheduledEventId,
                                          java.util.function.UnaryOperator<PendingActivity> f) {
    var pending = state.pendingActivities().get(scheduledEventId);
    return pending == null ? state
        : state.withActivities(RebuiltState.put(state.pendingActivities(), scheduledEventId, f.apply(pending)));
  }

  private static RebuiltState removeActivity(RebuiltState state, long scheduledEventId) {
    var next = RebuiltState.remove(state.pendingActivities(), scheduledEventId);
    return next == state.pendingActivities() ? state : state.withActivities(next);
  }

  private static RebuiltState mapChild(RebuiltState state, long initiatedEventId,
                                       java.util.function.UnaryOperator<PendingChild> f) {
    var pending = state.pendingChildren().get(initiatedEventId);
    return pending == null ? state
        : state.withChildren(RebuiltState.put(state.pendingChildren(), initiatedEventId, f.apply(pending)));
  }

  private static RebuiltState removeChild(RebuiltState state, long initiatedEventId) {
    var next = RebuiltState.remove(state.pendingChildren(), initiatedEventId);
    return next == state.pendingChildren() ? state : state.withChildren(next);
  }

  /**
   * SPEC-001 D1. The source mints a fresh UUID here, so replaying one history twice gives two
   * different states. This derives the id from the run and the initiating event instead, in the
   * version-5 shape, so replay is a function of its input.
   */
  public static String derivedRequestId(String runId, long initiatedEventId) {
    byte[] digest;
    try {
      var sha1 = MessageDigest.getInstance("SHA-1");
      sha1.update(runId.getBytes(StandardCharsets.UTF_8));
      sha1.update((byte) 0);
      sha1.update(Long.toString(initiatedEventId).getBytes(StandardCharsets.UTF_8));
      digest = sha1.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-1 is required by the platform", e);
    }
    digest[6] = (byte) ((digest[6] & 0x0f) | 0x50);
    digest[8] = (byte) ((digest[8] & 0x3f) | 0x80);
    var hex = new StringBuilder(36);
    for (int i = 0; i < 16; i++) {
      if (i == 4 || i == 6 || i == 8 || i == 10) {
        hex.append('-');
      }
      hex.append(String.format("%02x", digest[i]));
    }
    return hex.toString();
  }

  /**
   * SPEC-001 D3. What replay refuses to look at, reported separately: gaps, repeats and decreases
   * in the event ids of a history, named without changing what replay does with it.
   */
  public static Map<String, List<Long>> structure(List<HistoryEvent.Batch> history) {
    var gaps = new ArrayList<Long>();
    var repeats = new ArrayList<Long>();
    var decreases = new ArrayList<Long>();
    long previous = 0;
    boolean first = true;
    for (var b : history) {
      for (var e : b.events()) {
        if (!first) {
          if (e.eventId() == previous) {
            repeats.add(e.eventId());
          } else if (e.eventId() < previous) {
            decreases.add(e.eventId());
          } else if (e.eventId() > previous + 1) {
            gaps.add(previous + 1);
          }
        }
        previous = e.eventId();
        first = false;
      }
    }
    var out = new TreeMap<String, List<Long>>();
    out.put("gapsAfter", List.copyOf(gaps));
    out.put("repeatedEventIds", List.copyOf(repeats));
    out.put("decreasedToEventIds", List.copyOf(decreases));
    return out;
  }
}
