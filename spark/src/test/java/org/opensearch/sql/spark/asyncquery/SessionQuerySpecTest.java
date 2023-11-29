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
import org.opensearch.core.common.Strings;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.execution.statement.StatementModel;
import org.opensearch.sql.spark.execution.statement.StatementState;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class SessionQuerySpecTest extends AsyncQueryExecutorServiceSpec {
  @Test
  public void createAsyncQueryThenGetResultThenCancel() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    assertNotNull(response.getSessionId());
    Optional<StatementModel> statementModel =
        getStatement(stateStore, DATASOURCE).apply(response.getQueryId());
    assertTrue(statementModel.isPresent());
    assertEquals(StatementState.WAITING, statementModel.get().getStatementState());

    // 2. fetch async query result.
    AsyncQueryExecutionResponse asyncQueryResults =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
    assertTrue(Strings.isEmpty(asyncQueryResults.getError()));
    assertEquals(StatementState.WAITING.getState(), asyncQueryResults.getStatus());
    emrsClient.getJobRunResultCalled(0);

    // 3. cancel async query.
    String cancelQueryId = asyncQueryExecutorService.cancelQuery(response.getQueryId());
    StatementModel model =
        getStatementModelByQueryId(stateStore, new AsyncQueryId(response.getQueryId()));
    assertEquals(StatementState.CANCELLED, model.getStatementState());
    assertEquals(response.getQueryId(), cancelQueryId);
    emrsClient.cancelJobRunCalled(0);
  }

  @Test
  public void fetchResultSuccess() {
    LocalEMRSClient emrsClient = new LocalEMRSClient();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrsClient);

    // 1. create async query.
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest("select 1", DATASOURCE, LangType.SQL, null));
    assertNotNull(response.getSessionId());
    Optional<StatementModel> statementModel =
        getStatement(stateStore, DATASOURCE).apply(response.getQueryId());

    // update state
    updateStatementState(stateStore, DATASOURCE)
        .apply(statementModel.get(), StatementState.SUCCESS);
    // mock result
    writeQueryResult(response.getQueryId(), "SUCCESS", "");

    // 2. fetch async query result.
    AsyncQueryExecutionResponse result =
        asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
    assertEquals("SUCCESS", result.getStatus());
    assertNull(result.getError());
    assertEquals(0, result.getResults().size());
    emrsClient.getJobRunResultCalled(0);
  }
}
