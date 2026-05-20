/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.ResultSetAssertion;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.executor.QueryType;

public class DatetimeExtensionTest extends UnifiedQueryTestBase implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Override
  protected UnifiedQueryContext.Builder contextBuilder() {
    return UnifiedQueryContext.builder()
        .language(QueryType.PPL)
        .catalog(
            DEFAULT_CATALOG,
            new AbstractSchema() {
              @Override
              protected Map<String, Table> getTableMap() {
                return Map.of("events", createEventsTable());
              }
            });
  }

  @Before
  public void setUp() {
    super.setUp();
    compiler = new UnifiedQueryCompiler(context);
  }

  private Table createEventsTable() {
    return SimpleTable.builder()
        .col("id", INTEGER)
        .col("name", VARCHAR)
        .col("hire_date", DATE)
        .col("start_time", TIME)
        .col("created_at", TIMESTAMP)
        .row(new Object[] {1, "Alice", 19738, 43200000, 1705305600000L})
        .row(new Object[] {2, "Bob", 19894, 50400000, 1718841600000L})
        .build();
  }

  @Test
  public void testUdfResultNormalizedAndCastToVarchar() {
    var plan =
        givenQuery(
                """
                source = catalog.events \
                | eval d = DATE(name), t = TIME(name), ts = TIMESTAMP(name) \
                | fields d, t, ts\
                """)
            .assertPlan(
                """
                LogicalProject(d=[CAST($0):VARCHAR], t=[CAST($1):VARCHAR], ts=[CAST($2):VARCHAR])
                  LogicalProject(d=[DATE($1)], t=[TIME($1)], ts=[TIMESTAMP($1)])
                    LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    assertCallType(plan, "DATE", DATE);
    assertCallType(plan, "TIME", TIME, 9);
    assertCallType(plan, "TIMESTAMP", TIMESTAMP, 9);
  }

  @Test
  public void testNestedUdfCallsNormalized() {
    var plan =
        givenQuery("source = catalog.events | eval d = DATEDIFF(DATE(name), DATE(name)) | fields d")
            .assertPlan(
                """
                LogicalProject(d=[DATEDIFF(DATE($1), DATE($1))])
                  LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    assertCallType(plan, "DATE", DATE);
    assertCallType(plan, "DATEDIFF", BIGINT);
  }

  @Test
  public void testDateLiteralCastToVarchar() {
    var plan =
        givenQuery("source = catalog.events | eval d = DATE('2024-01-01') | fields d")
            .assertPlan(
                """
                LogicalProject(d=[CAST($0):VARCHAR])
                  LogicalProject(d=[DATE('2024-01-01':VARCHAR)])
                    LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    assertCallType(plan, "DATE", DATE);
  }

  @Test
  public void testFilterWithTimestampLiteral() {
    var plan =
        givenQuery(
                """
                source = catalog.events | where created_at > "2024-01-01T00:00:00Z" | fields id\
                """)
            .assertPlan(
                """
                LogicalProject(id=[$0])
                  LogicalFilter(condition=[>($4, TIMESTAMP('2024-01-01T00:00:00Z':VARCHAR))])
                    LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    assertCallType(plan, "TIMESTAMP", TIMESTAMP, 9);
  }

  @Test
  public void testComparisonWithDatetimeUdf() {
    var plan =
        givenQuery("source = catalog.events | where created_at < DATE(name) | fields id")
            .assertPlan(
                """
                LogicalProject(id=[$0])
                  LogicalFilter(condition=[<($4, TIMESTAMP(DATE($1)))])
                    LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    assertCallType(plan, "DATE", DATE);
    assertCallType(plan, "TIMESTAMP", TIMESTAMP, 9);
  }

  @Test
  public void testAllStandardDatetimeTypesCastToVarchar() {
    givenQuery("source = catalog.events | fields hire_date, start_time, created_at")
        .assertPlan(
            """
            LogicalProject(hire_date=[CAST($0):VARCHAR NOT NULL], start_time=[CAST($1):VARCHAR NOT NULL], created_at=[CAST($2):VARCHAR NOT NULL])
              LogicalProject(hire_date=[$2], start_time=[$3], created_at=[$4])
                LogicalTableScan(table=[[catalog, events]])
            """);
  }

  @Test
  public void testNonDatetimeFieldsNotWrapped() {
    givenQuery("source = catalog.events | fields id, name")
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1])
              LogicalTableScan(table=[[catalog, events]])
            """);
  }

  @Test
  public void testSequentialPlanCallsDoNotCorruptShuttleStack() {
    // Regression test: DatetimeUdtNormalizeRule extends RelHomogeneousShuttle which inherits a
    // stateful Deque<RelNode> stack field. Earlier implementations used a static INSTANCE shared
    // across all plan() calls; under workloads with aggregations (especially count + distinct_count
    // over datetime columns), the shared stack would desynchronize and visitChild's pop would throw
    // NoSuchElementException on subsequent plan calls. Running several distinct plans through the
    // same context confirms each invocation gets a fresh shuttle.
    for (int i = 0; i < 5; i++) {
      planner.plan(
          "source = catalog.events"
              + " | stats count() as field_count, distinct_count(created_at) as distinct_count");
      planner.plan(
          "source = catalog.events"
              + " | eval ts = TIMESTAMP(name)"
              + " | stats count() as field_count, distinct_count(ts) as distinct_count");
      planner.plan(
          "source = catalog.events | where created_at > \"2024-01-01\" | fields hire_date");
    }
  }

  @Test
  public void testOutputCastCanCompileAndExecute() throws Exception {
    RelNode plan =
        planner.plan("source = catalog.events | fields hire_date, start_time, created_at");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();
      verify(resultSet)
          .expectSchema(
              col("hire_date", java.sql.Types.VARCHAR),
              col("start_time", java.sql.Types.VARCHAR),
              col("created_at", java.sql.Types.VARCHAR))
          .expectData(
              row("2024-01-16", "12:00:00", "2024-01-15 08:00:00"),
              row("2024-06-20", "14:00:00", "2024-06-20 00:00:00"));
    }
  }

  private static void assertCallType(RelNode plan, String operatorName, SqlTypeName expectedType) {
    assertCallType(plan, operatorName, expectedType, -1);
  }

  private static void assertCallType(
      RelNode plan, String operatorName, SqlTypeName expectedType, int expectedPrecision) {
    AtomicReference<RexCall> ref = new AtomicReference<>();
    plan.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            visited.accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitCall(RexCall call) {
                    if (ref.get() == null
                        && call.getOperator().getName().equalsIgnoreCase(operatorName)) {
                      ref.set(call);
                    }
                    return super.visitCall(call);
                  }
                });
            return visited;
          }
        });
    assertNotNull("No RexCall found for: " + operatorName, ref.get());
    assertEquals(operatorName + " type", expectedType, ref.get().getType().getSqlTypeName());
    if (expectedPrecision >= 0) {
      assertEquals(
          operatorName + " precision", expectedPrecision, ref.get().getType().getPrecision());
    }
  }
}
