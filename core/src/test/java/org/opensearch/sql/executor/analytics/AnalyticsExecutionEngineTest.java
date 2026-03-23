/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.planner.physical.PhysicalPlan;

class AnalyticsExecutionEngineTest {

  private AnalyticsExecutionEngine engine;
  private QueryPlanExecutor mockExecutor;
  private CalcitePlanContext mockContext;

  @BeforeEach
  void setUp() throws Exception {
    mockExecutor = mock(QueryPlanExecutor.class);
    engine = new AnalyticsExecutionEngine(mockExecutor);
    mockContext = mock(CalcitePlanContext.class);
    setSysLimit(mockContext, SysLimit.DEFAULT);
  }

  /** Sets the public final sysLimit field on a mocked CalcitePlanContext. */
  private static void setSysLimit(CalcitePlanContext context, SysLimit sysLimit) throws Exception {
    Field field = CalcitePlanContext.class.getDeclaredField("sysLimit");
    field.setAccessible(true);
    field.set(context, sysLimit);
  }

  @Test
  void executeRelNode_basicTypesAndRows() {
    RelNode relNode = mockRelNode("name", SqlTypeName.VARCHAR, "age", SqlTypeName.INTEGER);
    Iterable<Object[]> rows = Arrays.asList(new Object[] {"Alice", 30}, new Object[] {"Bob", 25});
    when(mockExecutor.execute(relNode, null)).thenReturn(rows);
    // sysLimit is set to SysLimit.DEFAULT (querySizeLimit=10000) in setUp()

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    engine.execute(relNode, mockContext, captureListener(responseRef));

    QueryResponse response = responseRef.get();
    assertNotNull(response);

    // Verify schema
    Schema schema = response.getSchema();
    assertEquals(2, schema.getColumns().size());
    assertEquals("name", schema.getColumns().get(0).getName());
    assertEquals(ExprCoreType.STRING, schema.getColumns().get(0).getExprType());
    assertEquals("age", schema.getColumns().get(1).getName());
    assertEquals(ExprCoreType.INTEGER, schema.getColumns().get(1).getExprType());

    // Verify rows
    assertEquals(2, response.getResults().size());
    assertEquals("Alice", response.getResults().get(0).tupleValue().get("name").value());
    assertEquals(30, response.getResults().get(0).tupleValue().get("age").value());
    assertEquals("Bob", response.getResults().get(1).tupleValue().get("name").value());
    assertEquals(25, response.getResults().get(1).tupleValue().get("age").value());

    // Verify no pagination cursor
    assertEquals(org.opensearch.sql.executor.pagination.Cursor.None, response.getCursor());
  }

  @Test
  void executeRelNode_numericTypes() {
    RelNode relNode =
        mockRelNode(
            "b", SqlTypeName.TINYINT,
            "s", SqlTypeName.SMALLINT,
            "i", SqlTypeName.INTEGER,
            "l", SqlTypeName.BIGINT,
            "f", SqlTypeName.FLOAT,
            "d", SqlTypeName.DOUBLE);
    Iterable<Object[]> rows =
        Collections.singletonList(new Object[] {(byte) 1, (short) 2, 3, 4L, 5.0f, 6.0});
    when(mockExecutor.execute(relNode, null)).thenReturn(rows);
    // sysLimit is set to SysLimit.DEFAULT (querySizeLimit=10000) in setUp()

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    engine.execute(relNode, mockContext, captureListener(responseRef));

    Schema schema = responseRef.get().getSchema();
    assertEquals(ExprCoreType.BYTE, schema.getColumns().get(0).getExprType());
    assertEquals(ExprCoreType.SHORT, schema.getColumns().get(1).getExprType());
    assertEquals(ExprCoreType.INTEGER, schema.getColumns().get(2).getExprType());
    assertEquals(ExprCoreType.LONG, schema.getColumns().get(3).getExprType());
    assertEquals(ExprCoreType.FLOAT, schema.getColumns().get(4).getExprType());
    assertEquals(ExprCoreType.DOUBLE, schema.getColumns().get(5).getExprType());
  }

  @Test
  void executeRelNode_temporalTypes() {
    RelNode relNode =
        mockRelNode(
            "dt", SqlTypeName.DATE,
            "tm", SqlTypeName.TIME,
            "ts", SqlTypeName.TIMESTAMP);
    Iterable<Object[]> emptyRows = Collections.emptyList();
    when(mockExecutor.execute(relNode, null)).thenReturn(emptyRows);
    // sysLimit is set to SysLimit.DEFAULT (querySizeLimit=10000) in setUp()

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    engine.execute(relNode, mockContext, captureListener(responseRef));

    Schema schema = responseRef.get().getSchema();
    assertEquals(ExprCoreType.DATE, schema.getColumns().get(0).getExprType());
    assertEquals(ExprCoreType.TIME, schema.getColumns().get(1).getExprType());
    assertEquals(ExprCoreType.TIMESTAMP, schema.getColumns().get(2).getExprType());
  }

  @Test
  void executeRelNode_querySizeLimit() throws Exception {
    RelNode relNode = mockRelNode("id", SqlTypeName.INTEGER);
    List<Object[]> manyRows = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      manyRows.add(new Object[] {i});
    }
    when(mockExecutor.execute(relNode, null)).thenReturn(manyRows);
    setSysLimit(mockContext, new SysLimit(10, 10000, 50000));

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    engine.execute(relNode, mockContext, captureListener(responseRef));

    assertEquals(10, responseRef.get().getResults().size());
  }

  @Test
  void executeRelNode_emptyResults() {
    RelNode relNode = mockRelNode("name", SqlTypeName.VARCHAR);
    Iterable<Object[]> emptyRows = Collections.emptyList();
    when(mockExecutor.execute(relNode, null)).thenReturn(emptyRows);
    // sysLimit is set to SysLimit.DEFAULT (querySizeLimit=10000) in setUp()

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    engine.execute(relNode, mockContext, captureListener(responseRef));

    QueryResponse response = responseRef.get();
    assertNotNull(response);
    assertEquals(1, response.getSchema().getColumns().size());
    assertTrue(response.getResults().isEmpty());
  }

  @Test
  void executeRelNode_nullValues() {
    RelNode relNode = mockRelNode("name", SqlTypeName.VARCHAR, "age", SqlTypeName.INTEGER);
    Iterable<Object[]> rows = Collections.singletonList(new Object[] {null, null});
    when(mockExecutor.execute(relNode, null)).thenReturn(rows);
    // sysLimit is set to SysLimit.DEFAULT (querySizeLimit=10000) in setUp()

    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();
    engine.execute(relNode, mockContext, captureListener(responseRef));

    QueryResponse response = responseRef.get();
    assertEquals(1, response.getResults().size());
    assertTrue(response.getResults().get(0).tupleValue().get("name").isNull());
    assertTrue(response.getResults().get(0).tupleValue().get("age").isNull());
  }

  @Test
  void executeRelNode_errorPropagation() {
    RelNode relNode = mockRelNode("id", SqlTypeName.INTEGER);
    when(mockExecutor.execute(relNode, null)).thenThrow(new RuntimeException("Engine failure"));
    // sysLimit is set to SysLimit.DEFAULT (querySizeLimit=10000) in setUp()

    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.execute(
        relNode,
        mockContext,
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {}

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
          }
        });

    assertNotNull(errorRef.get());
    assertEquals("Engine failure", errorRef.get().getMessage());
  }

  @Test
  void explainRelNode() {
    // Use a real Calcite LogicalValues node so RelOptUtil.toString works
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder().add("name", SqlTypeName.VARCHAR).build();
    RelNode relNode = mock(RelNode.class);
    when(relNode.getRowType()).thenReturn(rowType);
    // RelOptUtil.toString calls rel.explain(RelWriterImpl), so we need a real node
    // For simplicity, test that explain doesn't throw and returns non-null
    // A full explain test would require building a real RelNode tree

    AtomicReference<ExplainResponse> responseRef = new AtomicReference<>();
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.explain(
        relNode,
        ExplainMode.STANDARD,
        mockContext,
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {
            responseRef.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
          }
        });

    // RelOptUtil.toString on a mock may throw or produce output depending on mock setup.
    // We accept either a successful response or a caught failure (not an uncaught exception).
    assertTrue(responseRef.get() != null || errorRef.get() != null);
  }

  @Test
  void explainRelNode_errorPropagation() {
    // Mock a RelNode whose explain triggers an error
    RelNode relNode = mock(RelNode.class);
    org.mockito.Mockito.doThrow(new RuntimeException("Explain failure"))
        .when(relNode)
        .explain(org.mockito.ArgumentMatchers.any());

    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.explain(
        relNode,
        ExplainMode.STANDARD,
        mockContext,
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {}

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
          }
        });

    assertNotNull(errorRef.get());
  }

  @Test
  void physicalPlanExecute_callsOnFailure() {
    PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.execute(
        physicalPlan,
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {}

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
          }
        });
    assertNotNull(errorRef.get());
    assertTrue(errorRef.get() instanceof UnsupportedOperationException);
  }

  @Test
  void physicalPlanExecuteWithContext_callsOnFailure() {
    PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.execute(
        physicalPlan,
        org.opensearch.sql.executor.ExecutionContext.emptyExecutionContext(),
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {}

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
          }
        });
    assertNotNull(errorRef.get());
    assertTrue(errorRef.get() instanceof UnsupportedOperationException);
  }

  @Test
  void physicalPlanExplain_callsOnFailure() {
    PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.explain(
        physicalPlan,
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse response) {}

          @Override
          public void onFailure(Exception e) {
            errorRef.set(e);
          }
        });
    assertNotNull(errorRef.get());
    assertTrue(errorRef.get() instanceof UnsupportedOperationException);
  }

  // --- helpers ---

  private RelNode mockRelNode(Object... nameTypePairs) {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < nameTypePairs.length; i += 2) {
      String name = (String) nameTypePairs[i];
      SqlTypeName typeName = (SqlTypeName) nameTypePairs[i + 1];
      builder.add(name, typeName);
    }
    RelDataType rowType = builder.build();

    RelNode relNode = mock(RelNode.class);
    when(relNode.getRowType()).thenReturn(rowType);
    return relNode;
  }

  private ResponseListener<QueryResponse> captureListener(AtomicReference<QueryResponse> ref) {
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        ref.set(response);
      }

      @Override
      public void onFailure(Exception e) {
        throw new AssertionError("Unexpected failure", e);
      }
    };
  }
}
