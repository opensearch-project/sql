/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.analytics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.BinaryType;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.SysLimit;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.QueryProfiling;
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

  /** Raw 16-byte ipv6-mapped buffer + IpType → canonical IP string + schema reports "ip". */
  @Test
  void executeRelNode_ipColumnRendersAsAddressString() {
    RelNode relNode = mockRelNodeWithType("host", new IpType(true));
    // 1.2.3.4 in ipv4-mapped-ipv6 form: 10 zero bytes + ff ff + 4 IPv4 bytes.
    byte[] ipv4 = new byte[16];
    ipv4[10] = (byte) 0xff;
    ipv4[11] = (byte) 0xff;
    ipv4[12] = 1;
    ipv4[13] = 2;
    ipv4[14] = 3;
    ipv4[15] = 4;
    // ::1 in pure ipv6 form.
    byte[] ipv6 = new byte[16];
    ipv6[15] = 1;
    Iterable<Object[]> rows = Arrays.asList(new Object[] {ipv4}, new Object[] {ipv6});
    stubExecutorWith(relNode, rows);

    QueryResponse response = executeAndCapture(relNode);
    String dump = dumpResponse(response);

    // Schema: column reports "ip", not "binary".
    assertEquals(ExprCoreType.IP, response.getSchema().getColumns().get(0).getExprType(), dump);
    // Cells: byte[] → formatted address string.
    assertEquals(
        "1.2.3.4",
        response.getResults().get(0).tupleValue().get("host").value(),
        "ipv4-mapped IPv6 buffer should render as dotted quad. " + dump);
    assertEquals(
        "::1",
        response.getResults().get(1).tupleValue().get("host").value(),
        "pure IPv6 buffer should render as RFC 5952 compressed form. " + dump);
  }

  /** Raw byte buffer + BinaryType → base64 string + schema reports "binary". */
  @Test
  void executeRelNode_binaryColumnRendersAsBase64() {
    RelNode relNode = mockRelNodeWithType("blob", new BinaryType(true));
    Iterable<Object[]> rows =
        Collections.singletonList(new Object[] {"Some binary blob".getBytes()});
    stubExecutorWith(relNode, rows);

    QueryResponse response = executeAndCapture(relNode);
    String dump = dumpResponse(response);

    assertEquals(ExprCoreType.BINARY, response.getSchema().getColumns().get(0).getExprType(), dump);
    assertEquals(
        "U29tZSBiaW5hcnkgYmxvYg==",
        response.getResults().get(0).tupleValue().get("blob").value(),
        "byte[] should base64-encode to match OpenSearch binary wire format. " + dump);
  }

  /** TIME-typed list elements arrive as "1970-01-01[ T]HH:mm:ss[.frac]" — strip the prefix. */
  @Test
  void executeRelNode_listOfStringStripsEpochDatePrefix() {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    RelDataType arrayOfVarchar = typeFactory.createArrayType(varchar, -1);
    RelDataType rowType = typeFactory.builder().add("time_list", arrayOfVarchar).build();
    RelNode relNode = mock(RelNode.class);
    when(relNode.getRowType()).thenReturn(rowType);
    java.util.List<String> input =
        Arrays.asList(
            "1970-01-01 19:36:22",
            "1970-01-01T02:05:25",
            "1970-01-01 12:34:56.123456789",
            "2020-10-13 13:00:00",
            "hello");
    Iterable<Object[]> rows = Collections.singletonList(new Object[] {input});
    stubExecutorWith(relNode, rows);

    QueryResponse response = executeAndCapture(relNode);
    String dump = dumpResponse(response);

    java.util.List<String> result =
        response.getResults().get(0).tupleValue().get("time_list").collectionValue().stream()
            .map(org.opensearch.sql.data.model.ExprValue::stringValue)
            .toList();
    assertEquals(
        Arrays.asList("19:36:22", "02:05:25", "12:34:56.123456789", "2020-10-13 13:00:00", "hello"),
        result,
        dump);
  }

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

  @Test
  @SuppressWarnings("unchecked")
  void executeRelNode_profilePreservedOnAsyncListener() throws Exception {
    ProfileContext expected = QueryProfiling.activate(true);

    RelNode relNode = mockRelNode("col", SqlTypeName.VARCHAR);
    Iterable<Object[]> rows = Collections.singletonList(new Object[] {"value"});

    // Fire the executor's listener on a different thread to simulate async dispatch
    doAnswer(
            inv -> {
              ActionListener<Iterable<Object[]>> al = inv.getArgument(2);
              Thread t = new Thread(() -> al.onResponse(rows));
              t.start();
              t.join();
              return null;
            })
        .when(mockExecutor)
        .execute(eq(relNode), any(), any(ActionListener.class));

    AtomicReference<ProfileContext> seen = new AtomicReference<>();
    engine.execute(
        relNode,
        mockContext,
        new ResponseListener<>() {
          @Override
          public void onResponse(QueryResponse response) {
            seen.set(QueryProfiling.current());
          }

          @Override
          public void onFailure(Exception e) {
            throw new AssertionError(e);
          }
        });

    try {
      assertSame(expected, seen.get(), "Profile context not restored on async listener thread");
    } finally {
      QueryProfiling.clear();
    }
  }

  // --- helpers ---

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

  /** Variant of {@link #mockRelNode} that accepts a pre-built RelDataType (e.g. UDTs). */
  private RelNode mockRelNodeWithType(String name, RelDataType type) {
    SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RelDataType rowType = typeFactory.builder().add(name, type).build();

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
