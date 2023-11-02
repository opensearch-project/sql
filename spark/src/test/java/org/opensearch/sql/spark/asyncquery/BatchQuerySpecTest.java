/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatement;
import static org.opensearch.sql.spark.execution.statestore.StateStore.getStatementModelByQueryId;
import static org.opensearch.sql.spark.execution.statestore.StateStore.updateStatementState;

import java.util.Optional;
import org.junit.Test;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class BatchQuerySpecTest extends AsyncQueryExecutorServiceSpec {
  private final String QUERY = "REFRESH INDEX covering ON mys3.default.http_logs";

  @Test
  public void testSubmitAndFetchAndCancel() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();

    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // 1.submit batch query
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(QUERY, DATASOURCE, LangType.SQL, null));
    assertNotNull(response.getQueryId());
    assertNull(response.getSessionId());

    // 2.fetch result
    AsyncQueryExecutionResponse asyncQueryResults =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());

    assertEquals("waiting", asyncQueryResults.getStatus());
    assertEquals("", asyncQueryResults.getError());
    emrsClient.getJobRunResultCalled(0);

    // 3.cancel query
    String queryId = asyncQueryExecutorService.cancelQuery(response.getQueryId());
    StatementModel statementModel =
        getStatementModelByQueryId(stateStore, new AsyncQueryId(response.getQueryId()));
    assertEquals(StatementState.CANCELLED, statementModel.getStatementState());
    assertEquals(response.getQueryId(), queryId);
    emrsClient.cancelJobRunCalled(0);
  }

  @Test
  public void fetchResultStateSuccess() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // 1.create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(QUERY, DATASOURCE, LangType.SQL, null));

    // update state to SUCCESS
    Optional<StatementModel> statementModel =
        getStatement(stateStore, DATASOURCE).apply(response.getQueryId());
    updateStatementState(stateStore, DATASOURCE)
        .apply(statementModel.get(), StatementState.SUCCESS);
    // mock result
    writeQueryResult(response.getQueryId(), "SUCCESS", "");

    AsyncQueryExecutionResponse result =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
    assertEquals("SUCCESS", result.getStatus());
    assertNull(result.getError());
    assertEquals(0, result.getResults().size());
  }

  @Test
  public void fetchResultStateFail() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // 1.create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(QUERY, DATASOURCE, LangType.SQL, null));

    // update state to FAILED
    Optional<StatementModel> statementModel =
        getStatement(stateStore, DATASOURCE).apply(response.getQueryId());
    updateStatementState(stateStore, DATASOURCE).apply(statementModel.get(), StatementState.FAILED);
    // mock result
    writeQueryResult(response.getQueryId(), "failed", "error");

    AsyncQueryExecutionResponse result =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
    assertEquals("failed", result.getStatus());
    assertEquals("error", result.getError());
    assertNull(result.getResults());
  }
}
