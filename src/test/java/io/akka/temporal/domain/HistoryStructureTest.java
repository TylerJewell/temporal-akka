package io.akka.temporal.domain;

import static io.akka.temporal.domain.Histories.batch;
import static io.akka.temporal.domain.Histories.signaled;
import static io.akka.temporal.domain.Histories.started;
import static io.akka.temporal.domain.Histories.wftScheduled;
import static io.akka.temporal.domain.Histories.wftStarted;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/** SPEC-001 D3: what replay declines to check, reported without changing what replay does. */
public class HistoryStructureTest {

  @Test
  public void namesGapsRepeatsAndDecreasesWithoutRejecting() {
    // One batch: R26's cross-batch guard would reject the repeat, and what is under test here
    // is the part replay declines to look at, not the part it does.
    var history = List.of(
        batch(List.of(started(1, 0), wftScheduled(2, 0), wftStarted(9, 0, 2), signaled(9, 0, "s"),
            signaled(4, 0, "s"))));

    var report = ReplayEngine.structure(history);
    assertEquals(List.of(3L), report.get("gapsAfter"));
    assertEquals(List.of(9L), report.get("repeatedEventIds"));
    assertEquals(List.of(4L), report.get("decreasedToEventIds"));

    // Replay itself takes the same history without complaint, and answers from its last event.
    assertEquals(5L, ReplayEngine.replay("run-1", history).nextEventId());
  }

  @Test
  public void aCleanHistoryReportsNothing() {
    var history = List.of(batch(List.of(started(1, 0), wftScheduled(2, 0))), batch(List.of(wftStarted(3, 0, 2))));
    var report = ReplayEngine.structure(history);
    assertEquals(List.of(), report.get("gapsAfter"));
    assertEquals(List.of(), report.get("repeatedEventIds"));
    assertEquals(List.of(), report.get("decreasedToEventIds"));
  }
}
