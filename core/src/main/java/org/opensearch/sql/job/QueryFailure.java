/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.job;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Neutral, client-safe description of a query failure.
 *
 * <p>The lifecycle layer stores this snapshot rather than the original {@link Throwable} so that
 * later observers cannot mutate the failure and no engine-specific exception types leak through the
 * public API.
 *
 * <p>{@code type} and {@code reason} are the summary fields that have always been captured. {@code
 * details} is an optional structured payload, in {@link org.json.JSONObject}-style map form,
 * populated by the engine's failure renderer so that an async GET on a {@code FAILED} job can
 * return the same structured {@code error} object the sync REST path would have returned. For
 * engines that do not supply a renderer, {@code details} is the empty map.
 *
 * @param type simple exception type
 * @param reason client-facing message
 * @param details structured error payload mirroring the sync REST error body; never {@code null}
 */
public record QueryFailure(String type, String reason, Map<String, Object> details) {

  /**
   * Validates the client-safe descriptor and defensively copies {@code details} so later mutation
   * of the caller's map does not leak into the snapshot. All three fields are required.
   *
   * @throws NullPointerException if {@code type}, {@code reason}, or {@code details} is {@code
   *     null}
   */
  public QueryFailure {
    Objects.requireNonNull(type, "type must not be null");
    Objects.requireNonNull(reason, "reason must not be null");
    Objects.requireNonNull(details, "details must not be null");
    details = Map.copyOf(details);
  }

  /**
   * Builds a snapshot from an exception thrown by a runner without a structured renderer. The
   * {@code details} field is the empty map.
   */
  public static QueryFailure of(Throwable throwable) {
    return of(throwable, null);
  }

  /**
   * Builds a snapshot from an exception thrown by a runner. {@code renderer} supplies the
   * structured {@code details} payload; when {@code null} or when the renderer itself throws, the
   * snapshot carries an empty details map so a renderer bug can never prevent the state machine
   * from reaching {@code FAILED}.
   */
  public static QueryFailure of(
      Throwable throwable, Function<Throwable, Map<String, Object>> renderer) {
    Objects.requireNonNull(throwable, "throwable must not be null");
    String type =
        throwable.getClass().getSimpleName().isBlank()
            ? throwable.getClass().getName()
            : throwable.getClass().getSimpleName();
    String message = throwable.getMessage();
    String reason = (message == null || message.isBlank()) ? "query execution failed" : message;
    Map<String, Object> details = Map.of();
    if (renderer != null) {
      try {
        Map<String, Object> rendered = renderer.apply(throwable);
        if (rendered != null) {
          details = rendered;
        }
      } catch (RuntimeException ignored) {
        // A misbehaving renderer must not block the state machine; fall back to empty details.
      }
    }
    return new QueryFailure(type, reason, details);
  }
}
