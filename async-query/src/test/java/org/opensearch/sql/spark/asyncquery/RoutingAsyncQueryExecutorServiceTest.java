/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.job.Principal;
import org.opensearch.sql.job.QueryJobId;
import org.opensearch.sql.job.QueryJobService;
import org.opensearch.sql.job.QueryJobState;
import org.opensearch.sql.job.QueryJobStatus;
import org.opensearch.sql.job.QueryResult;
import org.opensearch.sql.job.SecurityAdapter;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;

class RoutingAsyncQueryExecutorServiceTest {

  private static final QueryJobId JOB_ID = new QueryJobId("node-1", "ctx-1");
  private static final String SPARK_ID = "some-spark-job-id";

  @Test
  void getAsyncQueryResults_dispatchesToJobServiceForQueryJobId() {
    AsyncQueryExecutorService spark = mock(AsyncQueryExecutorService.class);
    QueryJobService jobService = mock(QueryJobService.class);
    SecurityAdapter security = mock(SecurityAdapter.class);
    when(security.current()).thenReturn(Principal.UNSECURED);
    when(jobService.get(eq(JOB_ID), any()))
        .thenReturn(
            new QueryJobStatus(
                JOB_ID,
                QueryJobState.SUCCEEDED,
                0L,
                OptionalLong.of(0L),
                OptionalLong.of(1L),
                Optional.empty(),
                Optional.of(
                    new QueryResult(
                        new Schema(List.of()),
                        List.of(),
                        org.opensearch.sql.executor.pagination.Cursor.None,
                        List.of(),
                        1L))));

    RoutingAsyncQueryExecutorService router =
        new RoutingAsyncQueryExecutorService(spark, jobService, security);
    AsyncQueryExecutionResponse response =
        router.getAsyncQueryResults(JOB_ID.encode(), new NullAsyncQueryRequestContext());
    assertEquals("SUCCEEDED", response.getStatus());
    verify(spark, never()).getAsyncQueryResults(any(), any());
  }

  @Test
  void getAsyncQueryResults_fallsBackToSparkForNonJobId() {
    AsyncQueryExecutorService spark = mock(AsyncQueryExecutorService.class);
    QueryJobService jobService = mock(QueryJobService.class);
    SecurityAdapter security = mock(SecurityAdapter.class);
    AsyncQueryExecutionResponse expected =
        new AsyncQueryExecutionResponse("RUNNING", null, null, null, null);
    when(spark.getAsyncQueryResults(eq(SPARK_ID), any(AsyncQueryRequestContext.class)))
        .thenReturn(expected);

    RoutingAsyncQueryExecutorService router =
        new RoutingAsyncQueryExecutorService(spark, jobService, security);
    assertEquals(
        expected, router.getAsyncQueryResults(SPARK_ID, new NullAsyncQueryRequestContext()));
    verify(jobService, never()).get(any(), any());
  }

  @Test
  void cancelQuery_dispatchesToJobServiceForQueryJobId() {
    AsyncQueryExecutorService spark = mock(AsyncQueryExecutorService.class);
    QueryJobService jobService = mock(QueryJobService.class);
    SecurityAdapter security = mock(SecurityAdapter.class);
    when(security.current()).thenReturn(Principal.UNSECURED);

    RoutingAsyncQueryExecutorService router =
        new RoutingAsyncQueryExecutorService(spark, jobService, security);
    String encoded = JOB_ID.encode();
    assertEquals(encoded, router.cancelQuery(encoded, new NullAsyncQueryRequestContext()));
    verify(jobService, times(1)).cancel(eq(JOB_ID), any());
    verify(spark, never()).cancelQuery(any(), any());
  }

  @Test
  void cancelQuery_fallsBackToSparkForNonJobId() {
    AsyncQueryExecutorService spark = mock(AsyncQueryExecutorService.class);
    QueryJobService jobService = mock(QueryJobService.class);
    SecurityAdapter security = mock(SecurityAdapter.class);
    when(spark.cancelQuery(eq(SPARK_ID), any(AsyncQueryRequestContext.class))).thenReturn(SPARK_ID);

    RoutingAsyncQueryExecutorService router =
        new RoutingAsyncQueryExecutorService(spark, jobService, security);
    assertEquals(SPARK_ID, router.cancelQuery(SPARK_ID, new NullAsyncQueryRequestContext()));
    verify(jobService, never()).cancel(any(), any());
  }

  @Test
  void createAsyncQuery_alwaysDelegatesToSpark() {
    AsyncQueryExecutorService spark = mock(AsyncQueryExecutorService.class);
    QueryJobService jobService = mock(QueryJobService.class);
    SecurityAdapter security = mock(SecurityAdapter.class);
    CreateAsyncQueryRequest req = mock(CreateAsyncQueryRequest.class);
    CreateAsyncQueryResponse resp = new CreateAsyncQueryResponse("qid", "sid");
    when(spark.createAsyncQuery(eq(req), any(AsyncQueryRequestContext.class))).thenReturn(resp);

    RoutingAsyncQueryExecutorService router =
        new RoutingAsyncQueryExecutorService(spark, jobService, security);
    assertEquals(resp, router.createAsyncQuery(req, new NullAsyncQueryRequestContext()));
  }
}
