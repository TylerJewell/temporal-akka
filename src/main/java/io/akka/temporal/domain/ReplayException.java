package io.akka.temporal.domain;

/**
 * A replay that cannot proceed. The message is the source's message verbatim, because the message
 * is part of what is being compared: two systems that reject the same history for different stated
 * reasons have not agreed.
 */
public class ReplayException extends RuntimeException {

  public ReplayException(String message) {
    super(message);
  }
}
