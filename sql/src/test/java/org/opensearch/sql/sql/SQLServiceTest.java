/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.DefaultQueryManager;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.domain.SQLQueryRequest;

@ExtendWith(MockitoExtension.class)
class SQLServiceTest {

  private static String QUERY = "/_plugins/_sql";

  private static String EXPLAIN = "/_plugins/_sql/_explain";

  private SQLService sqlService;

  private DefaultQueryManager queryManager;

  @Mock
  private QueryService queryService;

  @Mock
  private ExecutionEngine.Schema schema;

  @BeforeEach
  public void setUp() {
    queryManager = DefaultQueryManager.defaultQueryManager();
    sqlService = new SQLService(new SQLSyntaxParser(), queryManager,
        new QueryPlanFactory(queryService));
  }

  @AfterEach
  public void cleanup() throws InterruptedException {
    queryManager.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void canExecuteSqlQuery() {
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(queryService).execute(any(), any());

    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT 123", QUERY, "jdbc"),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });
  }

  @Test
  public void canExecuteCsvFormatRequest() {
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(queryService).execute(any(), any());

    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT 123", QUERY, "csv"),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });
  }

  @Test
  public void canExplainSqlQuery() {
    doAnswer(invocation -> {
      ResponseListener<ExplainResponse> listener = invocation.getArgument(1);
      listener.onResponse(new ExplainResponse(new ExplainResponseNode("Test")));
      return null;
    }).when(queryService).explain(any(), any());

    sqlService.explain(new SQLQueryRequest(new JSONObject(), "SELECT 123", EXPLAIN, "csv"),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });
  }

  @Test
  public void canCaptureErrorDuringExecution() {
    sqlService.execute(
        new SQLQueryRequest(new JSONObject(), "SELECT", QUERY, ""),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {
            fail();
          }

          @Override
          public void onFailure(Exception e) {
            assertNotNull(e);
          }
        });
  }

  @Test
  public void canCaptureErrorDuringExplain() {
    sqlService.explain(
        new SQLQueryRequest(new JSONObject(), "SELECT", EXPLAIN, ""),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            fail("Should fail as expected");
          }

          @Override
          public void onFailure(Exception e) {
            assertNotNull(e);
          }
        });
  }

}
