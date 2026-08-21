package io.akka.temporal.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.akka.temporal.domain.HistoryEvent;
import io.akka.temporal.domain.RebuiltState;
import io.akka.temporal.domain.ReplayEngine;
import io.akka.temporal.domain.ReplayException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * The port half of {@code temporal-port/bench/}. Reads {@code workloads.json}, replays every
 * workload through {@link ReplayEngine}, and writes {@code port_output.json} in the shape the Go
 * runner writes {@code source_output.json}.
 *
 * <pre>
 *   java -cp "target/classes;target/test-classes;$(cat target/bench-classpath.txt)" \
 *        io.akka.temporal.bench.BenchRunner ../temporal-port/bench
 * </pre>
 *
 * <p>Not a test: it writes a file outside the project and takes a directory, and a build that ran
 * it every time would be a build that rewrites its own comparison. It lives under the test sources
 * because it is not part of what the service ships.
 *
 * <p>Timing is of the fold alone — no HTTP, no journal, no serialization — because that is what
 * the Go side times.
 */
public final class BenchRunner {

  private static final int ROUNDS = 200;
  private static final int WARMUP = 2000;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    var benchDir = Path.of(args.length > 0 ? args[0] : "../temporal-port/bench");
    var workloads = (ArrayNode) MAPPER.readTree(Files.readString(benchDir.resolve("workloads.json")));

    var answers = MAPPER.createObjectNode();
    for (JsonNode w : workloads) {
      answers.set(w.get("name").asText(), answer(w));
    }
    Files.writeString(benchDir.resolve("port_output.json"),
        MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(answers) + "\n");
    System.out.println("wrote " + benchDir.resolve("port_output.json") + " — " + answers.size() + " workloads");
  }

  private static ObjectNode answer(JsonNode w) throws Exception {
    var kind = w.get("kind").asText();
    var out = MAPPER.createObjectNode();
    out.put("kind", kind);
    out.put("events", countEvents(w));
    var states = MAPPER.createArrayNode();

    switch (kind) {
      case "single", "sequence" -> {
        var error = trace(batches(w.get("batches")), states);
        if (error != null) {
          out.put("error", error);
        }
      }
      case "stepwise" -> {
        var error = trace(stepwise(batches(w.get("batches"))), states);
        if (error != null) {
          out.put("error", error);
        }
      }
      case "arrival-orders" -> {
        var prefix = events(w.get("prefix"));
        var rows = events(w.get("rows"));
        var labels = MAPPER.createArrayNode();
        var seen = new HashSet<String>();
        String error = null;
        for (int[] order : permutations(rows.size())) {
          var batch = new ArrayList<>(prefix);
          var label = new StringBuilder();
          for (int i : order) {
            batch.add(rows.get(i));
            label.append(i);
          }
          labels.add(label.toString());
          var one = MAPPER.createArrayNode();
          var e = trace(List.of(new HistoryEvent.Batch(batch)), one);
          if (e != null) {
            error = e;
            seen.add("error: " + e);
            continue;
          }
          states.add(one.get(one.size() - 1));
          seen.add(one.get(one.size() - 1).toString());
        }
        out.set("orderLabels", labels);
        out.put("distinctAnswers", seen.size());
        if (error != null) {
          out.put("error", error);
        }
        if (w.path("expectsDistinctAnswers").asBoolean() && seen.size() < 2) {
          throw new IllegalStateException(w.get("name").asText()
              + " declares that the answer moves with delivery order and it did not");
        }
      }
      default -> throw new IllegalStateException("unknown workload kind " + kind);
    }

    out.set("states", states);
    out.put("nanosPerReplay", time(w));
    return out;
  }

  /** Replays the batches and records the state after each one, stopping at the first refusal. */
  private static String trace(List<HistoryEvent.Batch> batches, ArrayNode states) {
    var state = RebuiltState.empty();
    for (var b : batches) {
      try {
        state = ReplayEngine.replayBatch("run-1", state, b);
      } catch (ReplayException e) {
        return e.getMessage();
      }
      states.add(MAPPER.valueToTree(canonical(state)));
    }
    return null;
  }

  /** The projection both sides are compared on, field for field. */
  private static ObjectNode canonical(RebuiltState s) {
    var o = MAPPER.createObjectNode();
    o.put("status", s.status().name());
    o.put("runState", s.runState().name());
    o.put("nextEventId", s.nextEventId());
    o.put("lastFirstEventId", s.lastFirstEventId());
    o.put("currentVersion", s.currentVersion());
    o.put("workflowTypeName", s.workflowTypeName());
    o.put("taskQueue", s.taskQueue());
    o.put("lastCompletedWorkflowTaskStartedEventId", s.lastCompletedWorkflowTaskStartedEventId());
    var activities = MAPPER.createArrayNode();
    s.pendingActivities().values().forEach(a -> {
      var n = MAPPER.createObjectNode();
      n.put("scheduledEventId", a.scheduledEventId());
      n.put("startedEventId", a.startedEventId());
      n.put("activityId", a.activityId());
      n.put("activityType", a.activityType());
      n.put("attempt", a.attempt());
      n.put("cancelRequested", a.cancelRequested());
      activities.add(n);
    });
    o.set("pendingActivities", activities);
    var timers = MAPPER.createArrayNode();
    s.pendingTimers().values().forEach(t -> {
      var n = MAPPER.createObjectNode();
      n.put("timerId", t.timerId());
      n.put("startedEventId", t.startedEventId());
      timers.add(n);
    });
    o.set("pendingTimers", timers);
    var children = MAPPER.createArrayNode();
    s.pendingChildren().values().forEach(c -> {
      var n = MAPPER.createObjectNode();
      n.put("initiatedEventId", c.initiatedEventId());
      n.put("startedEventId", c.startedEventId());
      n.put("workflowId", c.workflowId());
      n.put("workflowTypeName", c.workflowTypeName());
      children.add(n);
    });
    o.set("pendingChildren", children);
    var cancels = MAPPER.createArrayNode();
    s.pendingCancelExternal().keySet().forEach(cancels::add);
    o.set("pendingCancelExternal", cancels);
    var signals = MAPPER.createArrayNode();
    s.pendingSignalExternal().keySet().forEach(signals::add);
    o.set("pendingSignalExternal", signals);
    if (s.workflowTask() == null) {
      o.putNull("workflowTask");
    } else {
      var n = MAPPER.createObjectNode();
      n.put("scheduledEventId", s.workflowTask().scheduledEventId());
      n.put("startedEventId", s.workflowTask().startedEventId());
      n.put("attempt", s.workflowTask().attempt());
      o.set("workflowTask", n);
    }
    var versions = MAPPER.createArrayNode();
    s.versionHistory().forEach(v -> {
      var n = MAPPER.createObjectNode();
      n.put("eventId", v.eventId());
      n.put("version", v.version());
      versions.add(n);
    });
    o.set("versionHistory", versions);
    o.put("signalCount", s.signalCount());
    o.put("cancelRequested", s.cancelRequested());
    o.put("completionEventBatchId", s.completionEventBatchId());
    var keys = MAPPER.createArrayNode();
    s.searchAttributes().keySet().stream().sorted().forEach(keys::add);
    o.set("searchAttributeKeys", keys);
    return o;
  }

  // ---------- decoding ----------

  private static List<HistoryEvent.Batch> batches(JsonNode raw) throws Exception {
    var out = new ArrayList<HistoryEvent.Batch>();
    for (JsonNode b : raw) {
      out.add(new HistoryEvent.Batch(events(b)));
    }
    return out;
  }

  private static List<HistoryEvent> events(JsonNode raw) throws Exception {
    var out = new ArrayList<HistoryEvent>();
    if (raw == null || raw.isMissingNode()) {
      return out;
    }
    for (JsonNode e : raw) {
      out.add(MAPPER.treeToValue(e, HistoryEvent.class));
    }
    return out;
  }

  private static List<HistoryEvent.Batch> stepwise(List<HistoryEvent.Batch> batches) {
    var out = new ArrayList<HistoryEvent.Batch>();
    for (var b : batches) {
      for (var e : b.events()) {
        out.add(new HistoryEvent.Batch(List.of(e)));
      }
    }
    return out;
  }

  private static List<int[]> permutations(int n) {
    var out = new ArrayList<int[]>();
    var a = new int[n];
    for (int i = 0; i < n; i++) {
      a[i] = i;
    }
    permute(a, 0, out);
    return out;
  }

  private static void permute(int[] a, int k, List<int[]> out) {
    if (k == a.length) {
      out.add(a.clone());
      return;
    }
    for (int i = k; i < a.length; i++) {
      swap(a, k, i);
      permute(a, k + 1, out);
      swap(a, k, i);
    }
  }

  private static void swap(int[] a, int i, int j) {
    int t = a[i];
    a[i] = a[j];
    a[j] = t;
  }

  private static int countEvents(JsonNode w) {
    int n = 0;
    for (JsonNode b : w.path("batches")) {
      n += b.size();
    }
    n += w.path("prefix").size() + w.path("rows").size();
    return n;
  }

  /**
   * The best of {@link #ROUNDS} timed windows, each holding {@link #innerRounds} whole replays,
   * after {@link #WARMUP} untimed ones. A single replay of a small workload is shorter than the
   * clock this platform offers, and the minimum of many zeroes is zero.
   */
  private static long time(JsonNode w) throws Exception {
    var kind = w.get("kind").asText();
    List<HistoryEvent.Batch> batches;
    if ("arrival-orders".equals(kind)) {
      var all = new ArrayList<>(events(w.get("prefix")));
      all.addAll(events(w.get("rows")));
      batches = List.of(new HistoryEvent.Batch(all));
    } else if ("stepwise".equals(kind)) {
      batches = stepwise(batches(w.get("batches")));
    } else {
      batches = batches(w.get("batches"));
    }

    for (int i = 0; i < WARMUP; i++) {
      replayQuietly(batches);
    }
    int inner = innerRounds(batches);
    long best = Long.MAX_VALUE;
    for (int round = 0; round < ROUNDS; round++) {
      long start = System.nanoTime();
      for (int i = 0; i < inner; i++) {
        replayQuietly(batches);
      }
      long elapsed = (System.nanoTime() - start) / inner;
      if (elapsed < best) {
        best = elapsed;
      }
    }
    return best;
  }

  private static int innerRounds(List<HistoryEvent.Batch> batches) {
    int events = batches.stream().mapToInt(b -> b.events().size()).sum();
    return events > 100 ? 20 : 500;
  }

  private static void replayQuietly(List<HistoryEvent.Batch> batches) {
    var state = RebuiltState.empty();
    for (var b : batches) {
      try {
        state = ReplayEngine.replayBatch("run-1", state, b);
      } catch (ReplayException e) {
        return;
      }
    }
  }

  private BenchRunner() {}
}
