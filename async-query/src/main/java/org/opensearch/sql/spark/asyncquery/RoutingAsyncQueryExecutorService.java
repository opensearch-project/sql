/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.job.Principal;
import org.opensearch.sql.job.QueryFailure;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.job.QueryJobService;
import org.opensearch.sql.job.QueryJobState;
import org.opensearch.sql.job.QueryJobStatus;
import org.opensearch.sql.job.SecurityAdapter;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;

/**
 * Id-shape router in front of the existing Spark-backed async query executor.
 *
 * <p>{@link org.opensearch.sql.spark.transport.TransportGetAsyncQueryResultAction} and
 * {@link org.opensearch.sql.spark.transport.TransportCancelAsyncQueryRequestAction} keep going
 * through {@link AsyncQueryExecutorService}; the router picks the right backend by inspecting the
 * id. If the id parses as a {@link QueryJobId} it goes to the in-JVM {@link QueryJobService};
 * otherwise it goes to the existing Spark-backed impl.
 *
 * <p>Create-side stays Spark-only on this endpoint. In-JVM PPL submissions arrive via
 * {@code /_plugins/_ppl} (see {@code TransportPPLQueryAction}), not here. This keeps the endpoint
 * semantics of {@code POST /_plugins/_async_query} unchanged for existing clients.
 *
 * <p>See #5765 for the response shape contract this maps to.
 */
public final class RoutingAsyncQueryExecutorService implements AsyncQueryExecutorService {

  private static final Schema EMPTY_SCHEMA = new Schema(List.of());

  private final AsyncQueryExecutorService sparkBacked;
  private final QueryJobService jobService;
  private final SecurityAdapter security;

  /**
   * @param sparkBacked existing Spark / EMR-Serverless backed impl
   * @param jobService in-JVM job service — the new backend for QueryJobId submissions
   * @param security captures the caller identity on each get/cancel; per-request FGAC also happens
   *     at the transport action, this is the ownership check inside the store
   * @throws NullPointerException if any argument is {@code null}
   */
  public RoutingAsyncQueryExecutorService(
      AsyncQueryExecutorService sparkBacked, QueryJobService jobService, SecurityAdapter security) {
    this.sparkBacked = Objects.requireNonNull(sparkBacked, "sparkBacked must not be null");
    this.jobService = Objects.requireNonNull(jobService, "jobService must not be null");
    this.security = Objects.requireNonNull(security, "security must not be null");
  }

  @Override
  public CreateAsyncQueryResponse createAsyncQuery(
      CreateAsyncQueryRequest request, AsyncQueryRequestContext context) {
    // In-JVM PPL submissions come in via POST /_plugins/_ppl. Everything reaching this endpoint
    // targets the Spark backend.
    return sparkBacked.createAsyncQuery(request, context);
  }

  @Override
  public AsyncQueryExecutionResponse getAsyncQueryResults(
      String queryId, AsyncQueryRequestContext context) {
    return asJobId(queryId)
        .map(id -> toAsyncResponse(jobService.get(id, security.current())))
        .orElseGet(() -> sparkBacked.getAsyncQueryResults(queryId, context));
  }

  @Override
  public String cancelQuery(String queryId, AsyncQueryRequestContext context) {
    Optional<QueryJobId> maybeJobId = asJobId(queryId);
    if (maybeJobId.isPresent()) {
      jobService.cancel(maybeJobId.get(), security.current());
      return queryId;
    }
    return sparkBacked.cancelQuery(queryId, context);
  }

  /**
   * Parses {@code id} as a {@link QueryJobId} or returns empty if the id does not match the opaque
   * layout. Spark job ids never accidentally match the versioned, length-prefixed layout.
   */
  private static Optional<QueryJobId> asJobId(String id) {
    if (id == null || id.isBlank()) {
      return Optional.empty();
    }
    try {
      return Optional.of(QueryJobId.parse(id));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  /**
   * Projects a neutral {@link QueryJobStatus} into the response shape the async-query transport
   * actions already know how to format. Terminal SUCCEEDED carries schema and rows; RUNNING /
   * PENDING carry an empty result; FAILED carries a sanitized error string; CANCELLED is reported
   * as {@code CANCELLED} status.
   */
  private static AsyncQueryExecutionResponse toAsyncResponse(QueryJobStatus status) {
    if (status.state() == QueryJobState.SUCCEEDED && status.result().isPresent()) {
      return new AsyncQueryExecutionResponse(
          status.state().name(),
          status.result().get().schema(),
          status.result().get().rows(),
          null,
          null);
    }
    if (status.state() == QueryJobState.FAILED) {
      return new AsyncQueryExecutionResponse(
          status.state().name(),
          EMPTY_SCHEMA,
          List.of(),
          status.failure().map(QueryFailure::reason).orElse("query execution failed"),
          null);
    }
    // PENDING / RUNNING / CANCELLED — no rows, no error payload.
    return new AsyncQueryExecutionResponse(
        status.state().name(), EMPTY_SCHEMA, List.of(), null, null);
  }
}
