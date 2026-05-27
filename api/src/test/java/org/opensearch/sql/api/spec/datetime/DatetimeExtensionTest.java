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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
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
        .col("event_str", VARCHAR)
        .col("hire_date", DATE)
        .col("start_time", TIME)
        .col("created_at", TIMESTAMP)
        .row(new Object[] {1, "Alice", 19738, 43200000, 1705305600000L})
        .row(new Object[] {2, "Bob", 19894, 50400000, 1718841600000L})
        .build();
  }

  @Test
  public void testUdfResultNormalized() {
    givenQuery(
            """
            source = catalog.events \
            | eval d = DATE(event_str), t = TIME(event_str), ts = TIMESTAMP(event_str) \
            | fields d, t, ts\
            """)
        .assertPlan(
            """
            LogicalProject(d=[DATE($1)], t=[TIME($1)], ts=[TIMESTAMP($1)])
              LogicalTableScan(table=[[catalog, events]])
            """)
        .assertReturnType("DATE", DATE)
        .assertReturnType("TIME", TIME, 9)
        .assertReturnType("TIMESTAMP", TIMESTAMP, 9);
  }

  @Test
  public void testNestedUdfCallsNormalized() {
    givenQuery(
            "source = catalog.events | eval d = DATEDIFF(DATE(event_str), DATE(event_str)) | fields"
                + " d")
        .assertPlan(
            """
            LogicalProject(d=[DATEDIFF(DATE($1), DATE($1))])
              LogicalTableScan(table=[[catalog, events]])
            """)
        .assertReturnType("DATE", DATE)
        .assertReturnType("DATEDIFF", BIGINT);
  }

  @Test
  public void testDateLiteralNormalized() {
    givenQuery("source = catalog.events | eval d = DATE('2024-01-01') | fields d")
        .assertPlan(
            """
            LogicalProject(d=[DATE('2024-01-01':VARCHAR)])
              LogicalTableScan(table=[[catalog, events]])
            """)
        .assertReturnType("DATE", DATE);
  }

  @Test
  public void testFilterWithTimestampLiteral() {
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
        .assertReturnType("TIMESTAMP", TIMESTAMP, 9);
  }

  @Test
  public void testComparisonWithDatetimeUdf() {
    givenQuery("source = catalog.events | where created_at < DATE(event_str) | fields id")
        .assertPlan(
            """
            LogicalProject(id=[$0])
              LogicalFilter(condition=[<($4, TIMESTAMP(DATE($1)))])
                LogicalTableScan(table=[[catalog, events]])
            """)
        .assertReturnType("DATE", DATE)
        .assertReturnType("TIMESTAMP", TIMESTAMP, 9);
  }

  @Test
  public void testStandardDatetimeFieldsNotWrapped() {
    givenQuery("source = catalog.events | fields hire_date, start_time, created_at")
        .assertPlan(
            """
            LogicalProject(hire_date=[$2], start_time=[$3], created_at=[$4])
              LogicalTableScan(table=[[catalog, events]])
            """);
  }

  @Test
  public void testNonDatetimeFieldsNotWrapped() {
    givenQuery("source = catalog.events | fields id, event_str")
        .assertPlan(
            """
            LogicalProject(id=[$0], event_str=[$1])
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
              + " | eval ts = TIMESTAMP(event_str)"
              + " | stats count() as field_count, distinct_count(ts) as distinct_count");
      planner.plan(
          "source = catalog.events | where created_at > \"2024-01-01\" | fields hire_date");
    }
  }

  @Test
  public void testDatetimeFieldsPreserveStandardTypes() throws Exception {
    RelNode plan =
        planner.plan("source = catalog.events | fields hire_date, start_time, created_at");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();
      verify(resultSet)
          .expectSchema(
              col("hire_date", java.sql.Types.DATE),
              col("start_time", java.sql.Types.TIME),
              col("created_at", java.sql.Types.TIMESTAMP));
    }
  }
}
