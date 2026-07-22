/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests that the DatetimeExtension UDT-normalization post-analysis rule applies correctly to the
 * SQL V2 parser path through CalciteRelNodeVisitor.
 */
public class DatetimeExtensionSqlTest extends UnifiedQueryTestBase {

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Override
  protected UnifiedQueryContext.Builder contextBuilder() {
    return UnifiedQueryContext.builder()
        .language(QueryType.SQL)
        .catalog(
            DEFAULT_CATALOG,
            new AbstractSchema() {
              @Override
              protected Map<String, Table> getTableMap() {
                return Map.of("events", createEventsTable());
              }
            });
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
  public void testStandardDatetimeTypesNotWrapped() {
    givenQuery("SELECT * FROM catalog.events")
        .assertPlan(
            """
            LogicalTableScan(table=[[catalog, events]])
            """);
  }

  @Test
  public void testFilterWithTimestampLiteral() {
    givenQuery("SELECT * FROM catalog.events WHERE created_at > '2024-01-01T00:00:00Z'")
        .assertPlan(
            """
            LogicalFilter(condition=[>($4, TIMESTAMP('2024-01-01T00:00:00Z':VARCHAR))])
              LogicalTableScan(table=[[catalog, events]])
            """)
        .assertReturnType("TIMESTAMP", TIMESTAMP, 9);
  }

  @Test
  public void testComparisonWithDatetimeUdf() {
    givenQuery("SELECT * FROM catalog.events WHERE created_at < DATE(event_str)")
        .assertPlan(
            """
            LogicalFilter(condition=[<($4, TIMESTAMP(DATE($1)))])
              LogicalTableScan(table=[[catalog, events]])
            """)
        .assertReturnType("DATE", DATE)
        .assertReturnType("TIMESTAMP", TIMESTAMP, 9);
  }
}
