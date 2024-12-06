/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.DefaultQueryManager;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.execution.QueryPlanFactory;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;

@RunWith(MockitoJUnitRunner.class)
public class PPLServiceTest {

  private static final String QUERY = "/_plugins/_ppl";

  private static final String EXPLAIN = "/_plugins/_ppl/_explain";

  private PPLService pplService;

  private DefaultQueryManager queryManager;

  @Mock private QueryService queryService;

  @Mock private ExecutionEngine.Schema schema;

  /** Setup the test context. */
  @Before
  public void setUp() {
    queryManager = DefaultQueryManager.defaultQueryManager();

    pplService =
        new PPLService(new PPLSyntaxParser(), queryManager, new QueryPlanFactory(queryService));
  }

  @After
  public void cleanup() throws InterruptedException {
    queryManager.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testExecuteShouldPass() {
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              listener.onResponse(new QueryResponse(schema, Collections.emptyList(), Cursor.None));
              return null;
            })
        .when(queryService)
        .execute(any(), any());

    pplService.execute(
        new PPLQueryRequest("search source=t a=1", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {}

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExecuteCsvFormatShouldPass() {
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              listener.onResponse(new QueryResponse(schema, Collections.emptyList(), Cursor.None));
              return null;
            })
        .when(queryService)
        .execute(any(), any());

    pplService.execute(
        new PPLQueryRequest("search source=t a=1", null, QUERY, "csv"),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {}

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExplainShouldPass() {
    doAnswer(
            invocation -> {
              ResponseListener<ExplainResponse> listener = invocation.getArgument(1);
              listener.onResponse(new ExplainResponse(new ExplainResponseNode("test")));
              return null;
            })
        .when(queryService)
        .explain(any(), any());

    pplService.explain(
        new PPLQueryRequest("search source=t a=1", null, EXPLAIN),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse pplQueryResponse) {}

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExecuteWithIllegalQueryShouldBeCaughtByHandler() {
    pplService.execute(
        new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {}
        });
  }

  @Test
  public void testExplainWithIllegalQueryShouldBeCaughtByHandler() {
    pplService.explain(
        new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<>() {
          @Override
          public void onResponse(ExplainResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {}
        });
  }

  @Test
  public void testPrometheusQuery() {
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              listener.onResponse(new QueryResponse(schema, Collections.emptyList(), Cursor.None));
              return null;
            })
        .when(queryService)
        .execute(any(), any());

    pplService.execute(
        new PPLQueryRequest("source = prometheus.http_requests_total", null, QUERY),
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {}

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testInvalidPPLQuery() {
    pplService.execute(
        new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {}
        });
  }
}
