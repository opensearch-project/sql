/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static com.google.common.collect.ImmutableMap.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.common.setting.Settings.Key.SQL_CURSOR_KEEP_ALIVE;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.executor.protector.OpenSearchExecutionProtector;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan;
import org.opensearch.sql.planner.SerializablePlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.split.Split;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchExecutionEngineTest {

  @Mock private OpenSearchClient client;

  @Mock private OpenSearchExecutionProtector protector;

  @Mock private static ExecutionEngine.Schema schema;

  @Mock private ExecutionContext executionContext;

  @Mock private Split split;

  @BeforeEach
  void setUp() {
    doAnswer(
            invocation -> {
              // Run task immediately
              Runnable task = invocation.getArgument(0);
              task.run();
              return null;
            })
        .when(client)
        .schedule(any());
  }

  @Test
  void execute_successfully() {
    List<ExprValue> expected =
        Arrays.asList(
            tupleValue(of("name", "John", "age", 20)), tupleValue(of("name", "Allen", "age", 30)));
    FakePhysicalPlan plan = new FakePhysicalPlan(expected.iterator());
    when(protector.protect(plan)).thenReturn(plan);

    OpenSearchExecutionEngine executor =
        new OpenSearchExecutionEngine(client, protector, new PlanSerializer(null));
    List<ExprValue> actual = new ArrayList<>();
    executor.execute(
        plan,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            actual.addAll(response.getResults());
          }

          @Override
          public void onFailure(Exception e) {
            fail("Error occurred during execution", e);
          }
        });

    assertTrue(plan.hasOpen);
    assertEquals(expected, actual);
    assertTrue(plan.hasClosed);
  }

  @Test
  void execute_with_cursor() {
    List<ExprValue> expected =
        Arrays.asList(
            tupleValue(of("name", "John", "age", 20)), tupleValue(of("name", "Allen", "age", 30)));
    var plan = new FakePhysicalPlan(expected.iterator());
    when(protector.protect(plan)).thenReturn(plan);

    OpenSearchExecutionEngine executor =
        new OpenSearchExecutionEngine(client, protector, new PlanSerializer(null));
    List<ExprValue> actual = new ArrayList<>();
    executor.execute(
        plan,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            actual.addAll(response.getResults());
            assertTrue(response.getCursor().toString().startsWith("n:"));
          }

          @Override
          public void onFailure(Exception e) {
            fail("Error occurred during execution", e);
          }
        });

    assertEquals(expected, actual);
  }

  @Test
  void execute_with_failure() {
    PhysicalPlan plan = mock(PhysicalPlan.class);
    RuntimeException expected = new RuntimeException("Execution error");
    when(plan.hasNext()).thenThrow(expected);
    when(protector.protect(plan)).thenReturn(plan);

    OpenSearchExecutionEngine executor =
        new OpenSearchExecutionEngine(client, protector, new PlanSerializer(null));
    AtomicReference<Exception> actual = new AtomicReference<>();
    executor.execute(
        plan,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            fail("Expected error didn't happen");
          }

          @Override
          public void onFailure(Exception e) {
            actual.set(e);
          }
        });
    assertEquals(expected, actual.get());
    verify(plan).close();
  }

  @Test
  void explain_successfully() {
    OpenSearchExecutionEngine executor =
        new OpenSearchExecutionEngine(client, protector, new PlanSerializer(null));
    Settings settings = mock(Settings.class);
    when(settings.getSettingValue(SQL_CURSOR_KEEP_ALIVE)).thenReturn(TimeValue.timeValueMinutes(1));
    when(settings.getSettingValue(Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER)).thenReturn(true);
    when(settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE)).thenReturn(false);

    OpenSearchExprValueFactory exprValueFactory = mock(OpenSearchExprValueFactory.class);
    final var name = new OpenSearchRequest.IndexName("test");
    final int defaultQuerySize = 100;
    final int maxResultWindow = 10000;
    final var requestBuilder =
        new OpenSearchRequestBuilder(defaultQuerySize, exprValueFactory, settings);
    PhysicalPlan plan =
        new OpenSearchIndexScan(
            mock(OpenSearchClient.class),
            maxResultWindow,
            requestBuilder.build(
                name, maxResultWindow, settings.getSettingValue(SQL_CURSOR_KEEP_ALIVE), client));

    AtomicReference<ExplainResponse> result = new AtomicReference<>();
    executor.explain(
        plan,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExplainResponse response) {
            result.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail(e);
          }
        });

    assertNotNull(result.get());
  }

  @Test
  void explain_with_failure() {
    OpenSearchExecutionEngine executor =
        new OpenSearchExecutionEngine(client, protector, new PlanSerializer(null));
    PhysicalPlan plan = mock(PhysicalPlan.class);
    when(plan.accept(any(), any())).thenThrow(IllegalStateException.class);

    AtomicReference<Exception> result = new AtomicReference<>();
    executor.explain(
        plan,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExplainResponse response) {
            fail("Should fail as expected");
          }

          @Override
          public void onFailure(Exception e) {
            result.set(e);
          }
        });

    assertNotNull(result.get());
  }

  @Test
  void call_add_split_and_open_in_order() {
    List<ExprValue> expected =
        Arrays.asList(
            tupleValue(of("name", "John", "age", 20)), tupleValue(of("name", "Allen", "age", 30)));
    FakePhysicalPlan plan = new FakePhysicalPlan(expected.iterator());
    when(protector.protect(plan)).thenReturn(plan);
    when(executionContext.getSplit()).thenReturn(Optional.of(split));

    OpenSearchExecutionEngine executor =
        new OpenSearchExecutionEngine(client, protector, new PlanSerializer(null));
    List<ExprValue> actual = new ArrayList<>();
    executor.execute(
        plan,
        executionContext,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            actual.addAll(response.getResults());
          }

          @Override
          public void onFailure(Exception e) {
            fail("Error occurred during execution", e);
          }
        });

    assertTrue(plan.hasSplit);
    assertTrue(plan.hasOpen);
    assertEquals(expected, actual);
    assertTrue(plan.hasClosed);
  }

  @RequiredArgsConstructor
  private static class FakePhysicalPlan extends TableScanOperator implements SerializablePlan {
    private final Iterator<ExprValue> it;
    private boolean hasOpen;
    private boolean hasClosed;
    private boolean hasSplit;

    @Override
    public void readExternal(ObjectInput in) {}

    @Override
    public void writeExternal(ObjectOutput out) {}

    @Override
    public void open() {
      super.open();
      hasOpen = true;
    }

    @Override
    public void close() {
      super.close();
      hasClosed = true;
    }

    @Override
    public void add(Split split) {
      assertFalse(hasOpen);
      hasSplit = true;
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public ExprValue next() {
      return it.next();
    }

    @Override
    public ExecutionEngine.Schema schema() {
      return schema;
    }

    @Override
    public String explain() {
      return "explain";
    }
  }
}
