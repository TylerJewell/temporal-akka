package io.akka.temporal.api;

import akka.javasdk.annotations.Acl;
import akka.javasdk.annotations.http.Get;
import akka.javasdk.annotations.http.HttpEndpoint;
import akka.javasdk.annotations.http.Post;
import akka.http.javadsl.model.HttpResponse;
import akka.javasdk.CommandException;
import akka.javasdk.client.ComponentClient;
import akka.javasdk.http.HttpException;
import akka.javasdk.http.HttpResponses;
import io.akka.temporal.application.WorkflowHistoryEntity;
import io.akka.temporal.domain.HistoryEvent;
import io.akka.temporal.domain.RebuiltState;
import io.akka.temporal.domain.ReplayEngine;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * How a caller outside a test reaches history replay: hand over one persistence batch at a time
 * and read back the rebuilt state.
 *
 * <p>One batch per call is the source's own shape (question-log row 24) and is also what the
 * target's command-size limit requires — a batch past 1,048,485 bytes is rejected by the runtime,
 * loudly, with its own message (row T3, SPEC-001 D4).
 */
@HttpEndpoint("/replay")
@Acl(allow = @Acl.Matcher(principal = Acl.Principal.INTERNET))
public class ReplayEndpoint {

  /** A whole history in one request, for the case where the caller has it all to hand. */
  public record HistoryRequest(List<HistoryEvent.Batch> batches) {}

  /**
   * One entity per workflow run, so two runs of the same workflow never share a journal.
   *
   * <p>Both halves are percent-encoded before being joined, so a colon inside either name cannot
   * be read as the join. The pipe that would read better is not available: the runtime rejects it
   * as a reserved character, and the rejection reaches the caller as a command timeout rather than
   * as anything it could act on (question-log row T5).
   */
  private static String entityId(String workflowId, String runId) {
    return URLEncoder.encode(workflowId, StandardCharsets.UTF_8) + ":"
        + URLEncoder.encode(runId, StandardCharsets.UTF_8);
  }

  private final ComponentClient componentClient;

  public ReplayEndpoint(ComponentClient componentClient) {
    this.componentClient = componentClient;
  }

  /**
   * A refused batch comes back as 400 carrying the source's own message, because the message is
   * part of what the two systems are compared on. Left to the default it would surface as a bare
   * 400 with the text buried, or as a 500 with only a correlation id.
   */
  @Post("/{workflowId}/{runId}/batches")
  public HttpResponse replayBatch(String workflowId, String runId, HistoryEvent.Batch batch) {
    try {
      var state = componentClient
          .forEventSourcedEntity(entityId(workflowId, runId))
          .method(WorkflowHistoryEntity::replayBatch)
          .invoke(new WorkflowHistoryEntity.ReplayBatch(runId, batch));
      return HttpResponses.ok(state);
    } catch (CommandException e) {
      throw HttpException.badRequest(e.getMessage());
    }
  }

  @Get("/{workflowId}/{runId}")
  public HttpResponse read(String workflowId, String runId) {
    return HttpResponses.ok(componentClient
        .forEventSourcedEntity(entityId(workflowId, runId))
        .method(WorkflowHistoryEntity::read)
        .invoke());
  }

  /**
   * Replays a whole history without keeping it, so a caller comparing two implementations does not
   * have to pick an entity id it will never read again.
   */
  @Post("/direct/{runId}")
  public RebuiltState replayDirect(String runId, HistoryRequest request) {
    return ReplayEngine.replay(runId, request.batches());
  }

  /**
   * SPEC-001 D3. Replay accepts a history whose event ids have gaps, repeats or decreases exactly
   * as the source does; this reports them instead, so the observation is available without
   * changing what replay decides.
   */
  @Post("/structure")
  public Map<String, List<Long>> structure(HistoryRequest request) {
    return ReplayEngine.structure(request.batches());
  }
}
