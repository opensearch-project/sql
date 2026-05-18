/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.planner.physical.PhysicalPlan;

class AnalyticsExecutionEngineTest {

  private AnalyticsExecutionEngine engine;

  @SuppressWarnings("unchecked")
  private QueryPlanExecutor<RelNode, Iterable<Object[]>> mockExecutor;

  private CalcitePlanContext mockContext;

  @BeforeEach
  void setUp() throws Exception {
    mockExecutor = (QueryPlanExecutor<RelNode, Iterable<Object[]>>) mock(QueryPlanExecutor.class);
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

  /** QueryPlanExecutor became async in analytics-framework 3.7 — stub the listener callback. */
  @SuppressWarnings("unchecked")
  private void stubExecutorWith(RelNode relNode, Iterable<Object[]> rows) {
    doAnswer(
            inv -> {
              ((ActionListener<Iterable<Object[]>>) inv.getArgument(2)).onResponse(rows);
              return null;
            })
        .when(mockExecutor)
        .execute(eq(relNode), any(), any(ActionListener.class));
  }

  @SuppressWarnings("unchecked")
  private void stubExecutorWithError(RelNode relNode, Exception error) {
    doAnswer(
            inv -> {
              ((ActionListener<Iterable<Object[]>>) inv.getArgument(2)).onFailure(error);
              return null;
            })
        .when(mockExecutor)
        .execute(eq(relNode), any(), any(ActionListener.class));
  }

  @Test
  void executeRelNode_basicTypesAndRows() {
    RelNode relNode = mockRelNode("name", SqlTypeName.VARCHAR, "age", SqlTypeName.INTEGER);
    Iterable<Object[]> rows = Arrays.asList(new Object[] {"Alice", 30}, new Object[] {"Bob", 25});
    stubExecutorWith(relNode, rows);

    QueryResponse response = executeAndCapture(relNode);
    String dump = dumpResponse(response);

    // Schema: 2 columns [name:STRING, age:INTEGER]
    assertEquals(2, response.getSchema().getColumns().size(), "Column count. " + dump);
    assertEquals("name", response.getSchema().getColumns().get(0).getName(), dump);
    assertEquals(ExprCoreType.STRING, response.getSchema().getColumns().get(0).getExprType(), dump);
    assertEquals("age", response.getSchema().getColumns().get(1).getName(), dump);
    assertEquals(
        ExprCoreType.INTEGER, response.getSchema().getColumns().get(1).getExprType(), dump);

    // Rows: [{name=Alice, age=30}, {name=Bob, age=25}]
    assertEquals(2, response.getResults().size(), "Row count. " + dump);
    assertEquals(
        "Alice", response.getResults().get(0).tupleValue().get("name").value(), "Row 0. " + dump);
    assertEquals(
        30, response.getResults().get(0).tupleValue().get("age").value(), "Row 0. " + dump);
    assertEquals(
        "Bob", response.getResults().get(1).tupleValue().get("name").value(), "Row 1. " + dump);
    assertEquals(
        25, response.getResults().get(1).tupleValue().get("age").value(), "Row 1. " + dump);

    // Cursor: None
    assertEquals(org.opensearch.sql.executor.pagination.Cursor.None, response.getCursor(), dump);
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
    stubExecutorWith(relNode, rows);

    QueryResponse response = executeAndCapture(relNode);
    String dump = dumpResponse(response);

    assertEquals(ExprCoreType.BYTE, response.getSchema().getColumns().get(0).getExprType(), dump);
    assertEquals(ExprCoreType.SHORT, response.getSchema().getColumns().get(1).getExprType(), dump);
    assertEquals(
        ExprCoreType.INTEGER, response.getSchema().getColumns().get(2).getExprType(), dump);
    assertEquals(ExprCoreType.LONG, response.getSchema().getColumns().get(3).getExprType(), dump);
    assertEquals(ExprCoreType.FLOAT, response.getSchema().getColumns().get(4).getExprType(), dump);
    assertEquals(ExprCoreType.DOUBLE, response.getSchema().getColumns().get(5).getExprType(), dump);

    // Verify actual values
    assertEquals(
        (byte) 1,
        response.getResults().get(0).tupleValue().get("b").value(),
        "byte value. " + dump);
    assertEquals(
        (short) 2,
        response.getResults().get(0).tupleValue().get("s").value(),
        "short value. " + dump);
    assertEquals(
        3, response.getResults().get(0).tupleValue().get("i").value(), "int value. " + dump);
    assertEquals(
        4L, response.getResults().get(0).tupleValue().get("l").value(), "long value. " + dump);
    assertEquals(
        5.0f, response.getResults().get(0).tupleValue().get("f").value(), "float value. " + dump);
    assertEquals(
        6.0, response.getResults().get(0).tupleValue().get("d").value(), "double value. " + dump);
  }

  @Test
  void executeRelNode_temporalTypes() {
    RelNode relNode =
        mockRelNode("dt", SqlTypeName.DATE, "tm", SqlTypeName.TIME, "ts", SqlTypeName.TIMESTAMP);
    Iterable<Object[]> emptyRows = Collections.emptyList();
    stubExecutorWith(relNode, emptyRows);

    QueryResponse response = executeAndCapture(relNode);
    String dump = dumpResponse(response);

    assertEquals(ExprCoreType.DATE, response.getSchema().getColumns().get(0).getExprType(), dump);
    assertEquals(ExprCoreType.TIME, response.getSchema().getColumns().get(1).getExprType(), dump);
    assertEquals(
        ExprCoreType.TIMESTAMP, response.getSchema().getColumns().get(2).getExprType(), dump);
    assertEquals(0, response.getResults().size(), "Should have 0 rows. " + dump);
  }

  // Query size limit is now enforced in the RelNode plan (LogicalSystemLimit) before it reaches
  // AnalyticsExecutionEngine. The engine trusts the executor to honor the limit.

  @Test
  void executeRelNode_emptyResults() {
    RelNode relNode = mockRelNode("name", SqlTypeName.VARCHAR);
    Iterable<Object[]> emptyRows = Collections.emptyList();
    stubExecutorWith(relNode, emptyRows);

    QueryResponse response = executeAndCapture(relNode);
    String dump = dumpResponse(response);

    assertEquals(1, response.getSchema().getColumns().size(), "Schema column count. " + dump);
    assertEquals(0, response.getResults().size(), "Row count should be 0. " + dump);
  }

  @Test
  void executeRelNode_nullValues() {
    RelNode relNode = mockRelNode("name", SqlTypeName.VARCHAR, "age", SqlTypeName.INTEGER);
    Iterable<Object[]> rows = Collections.singletonList(new Object[] {null, null});
    stubExecutorWith(relNode, rows);

    QueryResponse response = executeAndCapture(relNode);
    String dump = dumpResponse(response);

    assertEquals(1, response.getResults().size(), "Row count. " + dump);
    assertTrue(
        response.getResults().get(0).tupleValue().get("name").isNull(),
        "name should be null. " + dump);
    assertTrue(
        response.getResults().get(0).tupleValue().get("age").isNull(),
        "age should be null. " + dump);
  }

  @Test
  void executeRelNode_errorPropagation() {
    RelNode relNode = mockRelNode("id", SqlTypeName.INTEGER);
    stubExecutorWithError(relNode, new RuntimeException("Engine failure"));

    Exception error = executeAndCaptureError(relNode);
    System.out.println(dumpError("executeRelNode_errorPropagation", error));

    assertEquals(
        "Engine failure",
        error.getMessage(),
        "Exception type: " + error.getClass().getSimpleName() + ", message: " + error.getMessage());
  }

  @Test
  void physicalPlanExecute_callsOnFailure() {
    PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.execute(physicalPlan, failureListener(errorRef));

    assertNotNull(errorRef.get(), "onFailure should have been called");
    System.out.println(dumpError("physicalPlanExecute_callsOnFailure", errorRef.get()));
    assertTrue(
        errorRef.get() instanceof UnsupportedOperationException,
        "Expected UnsupportedOperationException, got: "
            + errorRef.get().getClass().getSimpleName()
            + " - "
            + errorRef.get().getMessage());
  }

  @Test
  void physicalPlanExecuteWithContext_callsOnFailure() {
    PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.execute(
        physicalPlan,
        org.opensearch.sql.executor.ExecutionContext.emptyExecutionContext(),
        failureListener(errorRef));

    assertNotNull(errorRef.get(), "onFailure should have been called");
    System.out.println(dumpError("physicalPlanExecuteWithContext_callsOnFailure", errorRef.get()));
    assertTrue(
        errorRef.get() instanceof UnsupportedOperationException,
        "Expected UnsupportedOperationException, got: "
            + errorRef.get().getClass().getSimpleName()
            + " - "
            + errorRef.get().getMessage());
  }

  @Test
  void physicalPlanExplain_callsOnFailure() {
    PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
    AtomicReference<Exception> errorRef = new AtomicReference<>();
    engine.explain(physicalPlan, explainFailureListener(errorRef));

    assertNotNull(errorRef.get(), "onFailure should have been called");
    System.out.println(dumpError("physicalPlanExplain_callsOnFailure", errorRef.get()));
    assertTrue(
        errorRef.get() instanceof UnsupportedOperationException,
        "Expected UnsupportedOperationException, got: "
            + errorRef.get().getClass().getSimpleName()
            + " - "
            + errorRef.get().getMessage());
  }

  // --- schema recovery: structural detection of CAST(<datetime> AS VARCHAR) projects ---

  @Test
  void buildSchema_recoversDatetimeLabelsFromOutputCastProject() {
    RelNode plan =
        buildOutputCastPlan(
            new String[] {"ts", "d", "t"},
            new SqlTypeName[] {SqlTypeName.TIMESTAMP, SqlTypeName.DATE, SqlTypeName.TIME});
    Iterable<Object[]> rows =
        Collections.singletonList(new Object[] {"2024-01-15 10:30:00", "2024-01-15", "10:30:00"});
    stubExecutorWith(plan, rows);

    QueryResponse response = executeAndCapture(plan);
    String dump = dumpResponse(response);

    assertEquals(
        ExprCoreType.TIMESTAMP, response.getSchema().getColumns().get(0).getExprType(), dump);
    assertEquals(ExprCoreType.DATE, response.getSchema().getColumns().get(1).getExprType(), dump);
    assertEquals(ExprCoreType.TIME, response.getSchema().getColumns().get(2).getExprType(), dump);
  }

  @Test
  void buildSchema_walksThroughLogicalSortWrapper() {
    RelNode castProject =
        buildOutputCastPlan(new String[] {"ts"}, new SqlTypeName[] {SqlTypeName.TIMESTAMP});
    // Mimic the LogicalSystemLimit wrapper that wraps the rule-emitted Project at the root.
    RexBuilder rexBuilder = castProject.getCluster().getRexBuilder();
    RexNode fetch =
        rexBuilder.makeLiteral(
            10000, castProject.getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER));
    RelNode wrapped = LogicalSort.create(castProject, RelCollations.EMPTY, null, fetch);
    stubExecutorWith(wrapped, Collections.emptyList());

    QueryResponse response = executeAndCapture(wrapped);
    String dump = dumpResponse(response);

    assertEquals(
        ExprCoreType.TIMESTAMP, response.getSchema().getColumns().get(0).getExprType(), dump);
  }

  @Test
  void buildSchema_projectWithUserExpressionDoesNotRecover() {
    // A Project that mixes a CAST(<datetime> AS VARCHAR) slot with a user-authored
    // expression slot (here: ts + INTERVAL — an unrelated function call) is NOT the
    // rule's emit shape. Recovery must NOT happen; the wire schema reflects the
    // Project's row type as-is (the cast slot stays VARCHAR/STRING).
    RelNode plan =
        buildMixedProject(
            new String[] {"ts_str", "calc"},
            new SqlTypeName[] {SqlTypeName.TIMESTAMP, SqlTypeName.INTEGER});
    stubExecutorWith(plan, Collections.emptyList());

    QueryResponse response = executeAndCapture(plan);
    String dump = dumpResponse(response);

    // ts_str slot is CAST(<TIMESTAMP> AS VARCHAR) but sits next to a user expression,
    // so the structural shape doesn't match — schema stays STRING (VARCHAR).
    assertEquals(ExprCoreType.STRING, response.getSchema().getColumns().get(0).getExprType(), dump);
  }

  @Test
  void buildSchema_nonProjectRootKeepsFieldType() {
    // When the rule didn't fire (no datetime fields), the root is whatever the
    // planner produced — the recovery path must fall back to the field type.
    RelNode plan = mockRelNode("name", SqlTypeName.VARCHAR, "age", SqlTypeName.INTEGER);
    stubExecutorWith(plan, Collections.emptyList());

    QueryResponse response = executeAndCapture(plan);
    String dump = dumpResponse(response);

    assertEquals(ExprCoreType.STRING, response.getSchema().getColumns().get(0).getExprType(), dump);
    assertEquals(
        ExprCoreType.INTEGER, response.getSchema().getColumns().get(1).getExprType(), dump);
  }

  // --- helpers ---

  /**
   * Builds a {@code LogicalProject(CAST(<typed> AS VARCHAR))} over a {@link LogicalValues} input —
   * mirrors what {@code DatetimeOutputCastRule} emits at the root.
   */
  private RelNode buildOutputCastPlan(String[] names, SqlTypeName[] types) {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    RelDataTypeFactory.Builder rowBuilder = typeFactory.builder();
    for (int i = 0; i < names.length; i++) {
      rowBuilder.add(names[i], types[i]).nullable(true);
    }
    RelDataType rowType = rowBuilder.build();
    LogicalValues input = LogicalValues.createEmpty(cluster, rowType);

    RelDataType varchar =
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
    List<RexNode> projects = new ArrayList<>(names.length);
    List<String> projectNames = new ArrayList<>(names.length);
    for (int i = 0; i < names.length; i++) {
      RexNode ref = rexBuilder.makeInputRef(input, i);
      projects.add(rexBuilder.makeCast(varchar, ref));
      projectNames.add(names[i]);
    }
    return LogicalProject.create(input, List.of(), projects, projectNames);
  }

  /**
   * Builds a {@code LogicalProject} where the first slot is {@code CAST(<datetime> AS VARCHAR)} and
   * the second slot is a user-authored expression ({@code col + 1}) — i.e. NOT the shape that the
   * rule emits.
   */
  private RelNode buildMixedProject(String[] names, SqlTypeName[] types) {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

    RelDataTypeFactory.Builder rowBuilder = typeFactory.builder();
    for (int i = 0; i < names.length; i++) {
      rowBuilder.add(names[i], types[i]).nullable(true);
    }
    LogicalValues input = LogicalValues.createEmpty(cluster, rowBuilder.build());

    RelDataType varchar =
        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true);
    RexNode castSlot = rexBuilder.makeCast(varchar, rexBuilder.makeInputRef(input, 0));
    RexNode plusSlot =
        rexBuilder.makeCall(
            SqlStdOperatorTable.PLUS,
            rexBuilder.makeInputRef(input, 1),
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER)));
    return LogicalProject.create(
        input, List.of(), List.of(castSlot, plusSlot), List.of(names[0], names[1]));
  }

  private QueryResponse executeAndCapture(RelNode relNode) {
    AtomicReference<QueryResponse> ref = new AtomicReference<>();
    engine.execute(relNode, mockContext, captureListener(ref));
    assertNotNull(ref.get(), "QueryResponse should not be null");
    // Always print the full response so test output shows exact results
    System.out.println(dumpResponse(ref.get()));
    return ref.get();
  }

  private Exception executeAndCaptureError(RelNode relNode) {
    AtomicReference<Exception> ref = new AtomicReference<>();
    engine.execute(
        relNode,
        mockContext,
        new ResponseListener<QueryResponse>() {
          @Override
          public void onResponse(QueryResponse response) {}

          @Override
          public void onFailure(Exception e) {
            ref.set(e);
          }
        });
    assertNotNull(ref.get(), "onFailure should have been called");
    return ref.get();
  }

  private ResponseListener<QueryResponse> failureListener(AtomicReference<Exception> ref) {
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {}

      @Override
      public void onFailure(Exception e) {
        ref.set(e);
      }
    };
  }

  private ResponseListener<ExplainResponse> explainFailureListener(AtomicReference<Exception> ref) {
    return new ResponseListener<ExplainResponse>() {
      @Override
      public void onResponse(ExplainResponse response) {}

      @Override
      public void onFailure(Exception e) {
        ref.set(e);
      }
    };
  }

  private String dumpError(String testName, Exception e) {
    return "\n--- "
        + testName
        + " ---\n"
        + "Exception: "
        + e.getClass().getSimpleName()
        + "\n"
        + "Message: "
        + e.getMessage()
        + "\n--- End ---";
  }

  /** Dumps the full QueryResponse into a readable string for test output and assertion messages. */
  private String dumpResponse(QueryResponse response) {
    StringBuilder sb = new StringBuilder();
    sb.append("\n--- QueryResponse ---\n");

    sb.append("Schema: [");
    sb.append(
        response.getSchema().getColumns().stream()
            .map(c -> c.getName() + ":" + c.getExprType().typeName())
            .collect(Collectors.joining(", ")));
    sb.append("]\n");

    sb.append("Rows (").append(response.getResults().size()).append("):\n");
    for (int i = 0; i < response.getResults().size(); i++) {
      sb.append("  [").append(i).append("] ");
      sb.append(response.getResults().get(i).tupleValue());
      sb.append("\n");
    }

    sb.append("Cursor: ").append(response.getCursor()).append("\n");
    sb.append("--- End ---");
    return sb.toString();
  }

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
