/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.QueryExecution;
import org.opensearch.sql.executor.execution.QueryExecutionFactory;
import org.opensearch.sql.ppl.config.PPLServiceConfig;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@RunWith(MockitoJUnitRunner.class)
public class PPLServiceTest {

  private static String QUERY = "/_plugins/_ppl";

  private static String EXPLAIN = "/_plugins/_ppl/_explain";

  private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  private PPLService pplService;

  @Mock
  private QueryManager queryManager;

  @Mock
  private QueryExecutionFactory queryExecutionFactory;

  @Mock
  private ExecutionEngine.Schema schema;

  @Mock
  private QueryExecution queryExecution;

  /**
   * Setup the test context.
   */
  @Before
  public void setUp() {
    context.registerBean(QueryManager.class, () -> queryManager);
    context.registerBean(QueryExecutionFactory.class, () -> queryExecutionFactory);
    context.register(PPLServiceConfig.class);
    context.refresh();
    pplService = context.getBean(PPLService.class);
  }

  @Test
  public void testExecuteShouldPass() {
    when(queryExecutionFactory.create(any())).thenReturn(queryExecution);
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(queryManager).submitQuery(any(), any());

    pplService.execute(new PPLQueryRequest("search source=t a=1", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {

          }

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExecuteCsvFormatShouldPass() {
    when(queryExecutionFactory.create(any())).thenReturn(queryExecution);
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(queryManager).submitQuery(any(), any());

    pplService.execute(new PPLQueryRequest("search source=t a=1", null, QUERY, "csv"),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {
          }

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExplainShouldPass() {
    when(queryExecutionFactory.create(any())).thenReturn(queryExecution);
    doAnswer(invocation -> {
      ResponseListener<ExplainResponse> listener = invocation.getArgument(1);
      listener.onResponse(new ExplainResponse(new ExplainResponseNode("test")));
      return null;
    }).when(queryManager).submitQuery(any(), any());

    pplService.explain(new PPLQueryRequest("search source=t a=1", null, EXPLAIN),
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse pplQueryResponse) {
          }

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testExecuteWithIllegalQueryShouldBeCaughtByHandler() {
    pplService.execute(new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {

          }
        });
  }

  @Test
  public void testExplainWithIllegalQueryShouldBeCaughtByHandler() {
    pplService.explain(new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<>() {
          @Override
          public void onResponse(ExplainResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {

          }
        });
  }

  @Test
  public void testPrometheusQuery() {
    when(queryExecutionFactory.create(any())).thenReturn(queryExecution);
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(queryManager).submitQuery(any(), any());

    pplService.execute(new PPLQueryRequest("source = prometheus.http_requests_total", null, QUERY),
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {

          }

          @Override
          public void onFailure(Exception e) {
            Assert.fail();
          }
        });
  }

  @Test
  public void testInvalidPPLQuery() {
    pplService.execute(new PPLQueryRequest("search", null, QUERY),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse pplQueryResponse) {
            Assert.fail();
          }

          @Override
          public void onFailure(Exception e) {

          }
        });
  }
}
