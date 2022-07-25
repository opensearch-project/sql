/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponseNode;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.ppl.config.PPLServiceConfig;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@RunWith(MockitoJUnitRunner.class)
public class PPLServiceTest {
  private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  private PPLService pplService;

  @Mock
  private StorageEngine storageEngine;

  @Mock
  private ExecutionEngine executionEngine;

  @Mock
  private Table table;

  @Mock
  private PhysicalPlan plan;

  @Mock
  private ExecutionEngine.Schema schema;

  /**
   * Setup the test context.
   */
  @Before
  public void setUp() {
    when(table.getFieldTypes()).thenReturn(ImmutableMap.of("a", ExprCoreType.INTEGER));
    when(table.implement(any(), new PlanContext())).thenReturn(plan);
    when(storageEngine.getTable(any())).thenReturn(table);

    context.registerBean(StorageEngine.class, () -> storageEngine);
    context.registerBean(ExecutionEngine.class, () -> executionEngine);
    context.register(PPLServiceConfig.class);
    context.refresh();
    pplService = context.getBean(PPLService.class);
  }

  @Test
  public void testExecuteShouldPass() {
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(executionEngine).execute(any(), any());

    pplService.execute(new PPLQueryRequest("search source=t a=1", null, null),
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
    doAnswer(invocation -> {
      ResponseListener<QueryResponse> listener = invocation.getArgument(1);
      listener.onResponse(new QueryResponse(schema, Collections.emptyList()));
      return null;
    }).when(executionEngine).execute(any(), any());

    pplService.execute(new PPLQueryRequest("search source=t a=1", null, "/_plugins/_ppl", "csv"),
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
    doAnswer(invocation -> {
      ResponseListener<ExplainResponse> listener = invocation.getArgument(1);
      listener.onResponse(new ExplainResponse(new ExplainResponseNode("test")));
      return null;
    }).when(executionEngine).explain(any(), any());

    pplService.explain(new PPLQueryRequest("search source=t a=1", null, null),
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
    pplService.execute(new PPLQueryRequest("search", null, null),
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
    pplService.explain(new PPLQueryRequest("search", null, null),
        new ResponseListener<ExplainResponse>() {
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
  public void test() {
    pplService.execute(new PPLQueryRequest("search", null, null),
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
