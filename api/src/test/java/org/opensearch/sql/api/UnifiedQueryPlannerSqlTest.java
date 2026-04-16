/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryPlannerSqlTest extends UnifiedQueryTestBase {

  private final AbstractSchema testDeepSchema =
      new AbstractSchema() {
        @Override
        protected Map<String, Schema> getSubSchemaMap() {
          return Map.of("opensearch", testSchema);
        }
      };

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Test
  public void testSqlQueryPlanning() {
    givenQuery(
            """
            SELECT *
            FROM catalog.employees\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSqlSelectSpecificColumns() {
    givenQuery(
            """
            SELECT id, name
            FROM catalog.employees\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSqlFilterQueryPlanning() {
    givenQuery(
            """
            SELECT name
            FROM catalog.employees
            WHERE age > 30\
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[>($2, 30)])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSqlAggregateQueryPlanning() {
    givenQuery(
            """
            SELECT department, count(*) AS cnt
            FROM catalog.employees
            GROUP BY department\
            """)
        .assertPlan(
            """
            LogicalAggregate(group=[{0}], cnt=[COUNT()])
              LogicalProject(department=[$3])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSqlJoinQueryPlanning() {
    givenQuery(
            """
            SELECT a.id, b.name
            FROM catalog.employees a
            JOIN catalog.employees b ON a.id = b.age\
            """)
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$5])
              LogicalJoin(condition=[=($0, $6)], joinType=[inner])
                LogicalTableScan(table=[[catalog, employees]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSqlOrderByQueryPlanning() {
    givenQuery(
            """
            SELECT name
            FROM catalog.employees
            ORDER BY age DESC\
            """)
        .assertPlan(
            """
            LogicalProject(name=[$0])
              LogicalSort(sort0=[$1], dir0=[DESC])
                LogicalProject(name=[$1], age=[$2])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSqlSubqueryPlanning() {
    // Calcite represents scalar subqueries as $SCALAR_QUERY{...} with embedded plan text whose
    // formatting (whitespace, line breaks) may vary across versions. Assert output fields only.
    givenQuery(
            """
            SELECT name
            FROM catalog.employees
            WHERE age > (SELECT avg(age) FROM catalog.employees)\
            """)
        .assertFields("name");
  }

  @Test
  public void testSqlCteQueryPlanning() {
    // CTE is inlined by Calcite — same plan as a direct filter query
    givenQuery(
            """
            WITH seniors AS (
              SELECT name, age FROM catalog.employees WHERE age > 30
            )
            SELECT name
            FROM seniors\
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[>($2, 30)])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSqlQueryPlanningWithDefaultNamespace() {
    UnifiedQueryContext sqlContext =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog("opensearch", testSchema)
            .defaultNamespace("opensearch")
            .build();
    UnifiedQueryPlanner sqlPlanner = new UnifiedQueryPlanner(sqlContext);

    assertNotNull("Plan should be created", sqlPlanner.plan("SELECT * FROM opensearch.employees"));
    assertNotNull("Plan should be created", sqlPlanner.plan("SELECT * FROM employees"));
  }

  @Test
  public void testSqlQueryPlanningWithDefaultNamespaceMultiLevel() {
    UnifiedQueryContext sqlContext =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog("catalog", testDeepSchema)
            .defaultNamespace("catalog.opensearch")
            .build();
    UnifiedQueryPlanner sqlPlanner = new UnifiedQueryPlanner(sqlContext);

    assertNotNull(
        "Plan should be created", sqlPlanner.plan("SELECT * FROM catalog.opensearch.employees"));
    assertNotNull("Plan should be created", sqlPlanner.plan("SELECT * FROM employees"));

    assertThrows(
        IllegalStateException.class, () -> sqlPlanner.plan("SELECT * FROM opensearch.employees"));
  }

  @Test
  public void testSqlQueryPlanningWithMultipleCatalogs() {
    UnifiedQueryContext sqlContext =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .build();
    UnifiedQueryPlanner sqlPlanner = new UnifiedQueryPlanner(sqlContext);

    assertNotNull(
        "Plan should be created",
        sqlPlanner.plan(
            """
            SELECT a.id
            FROM catalog1.employees a
            JOIN catalog2.employees b ON a.id = b.id\
            """));
  }

  @Test
  public void testInvalidSqlThrowsException() {
    assertThrows(IllegalStateException.class, () -> planner.plan("SELECT FROM"));
  }

  @Test
  public void testNonQueryStatementsBlockedByWhitelist() {
    List.of(
            """
            INSERT INTO catalog.employees (id, name, age, department)
            VALUES (99, 'injected', 0, 'hacked')\
            """,
            """
            DELETE FROM catalog.employees
            WHERE age > 30\
            """,
            """
            UPDATE catalog.employees
            SET department = 'Fired'
            WHERE age > 50\
            """,
            """
            EXPLAIN PLAN FOR
            SELECT * FROM catalog.employees\
            """,
            """
            MERGE INTO catalog.employees AS t
            USING (SELECT 99 AS id) AS s ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET name = 'hacked'\
            """)
        .forEach(
            sql ->
                givenInvalidQuery(sql).assertErrorMessage("Only query statements are supported"));
  }

  @Test
  public void testNonQueryStatementsBlockedByParser() {
    // Babel parser rejects CREATE MATERIALIZED VIEW
    givenInvalidQuery(
            """
            CREATE MATERIALIZED VIEW mv AS
            SELECT department, count(*)
            FROM catalog.employees
            GROUP BY department\
            """)
        .assertErrorMessage("Encountered");

    // Babel parser accepts SHOW TABLES but it's blocked by query-type whitelist
    givenInvalidQuery(
            """
            SHOW TABLES\
            """)
        .assertErrorMessage("Only query statements are supported");
  }
}
