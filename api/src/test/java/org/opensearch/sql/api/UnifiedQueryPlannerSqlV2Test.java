/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for SQL query planning through the V2 ANTLR parser path. Covers SELECT, WHERE, ORDER BY,
 * JOIN, UNION, and MINUS operations that produce valid RelNode plans.
 */
public class UnifiedQueryPlannerSqlV2Test extends UnifiedQueryTestBase {

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Before
  @Override
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "employees", createEmployeesTable(),
                "departments", createDepartmentsTable());
          }
        };

    context = contextBuilder().build();
    planner = new UnifiedQueryPlanner(context);
  }

  private Table createDepartmentsTable() {
    return SimpleTable.builder()
        .col("dept_id", INTEGER)
        .col("dept_name", VARCHAR)
        .row(new Object[] {1, "Engineering"})
        .row(new Object[] {2, "Sales"})
        .row(new Object[] {3, "Marketing"})
        .build();
  }

  @Test
  public void selectStar() {
    givenQuery("SELECT * FROM catalog.employees")
        .assertPlan(
            """
            LogicalTableScan(table=[[catalog, employees]])
            """)
        .assertFields("id", "name", "age", "department");
  }

  @Test
  public void testFilter() {
    givenQuery("SELECT * FROM catalog.employees WHERE age > 30")
        .assertPlan(
            """
            LogicalFilter(condition=[>($2, 30)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testOrderBy() {
    givenQuery("SELECT * FROM catalog.employees ORDER BY age")
        .assertPlan(
            """
            LogicalSort(sort0=[$2], dir0=[ASC-nulls-first])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testFilterAndOrderBy() {
    givenQuery("SELECT * FROM catalog.employees WHERE name = 'Alice' ORDER BY age")
        .assertPlan(
            """
            LogicalSort(sort0=[$2], dir0=[ASC-nulls-first])
              LogicalFilter(condition=[=($1, 'Alice')])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testJoinTypes() {
    Map.of("JOIN", "inner", "LEFT JOIN", "left", "RIGHT JOIN", "right")
        .forEach(
            (syntax, type) ->
                givenQuery(
                        """
                        SELECT * FROM catalog.employees %s catalog.departments
                          ON employees.department = departments.dept_name
                        """
                            .formatted(syntax))
                    .assertPlan(
                        """
                        LogicalJoin(condition=[=($3, $5)], joinType=[%s])
                          LogicalTableScan(table=[[catalog, employees]])
                          LogicalTableScan(table=[[catalog, departments]])
                        """
                            .formatted(type)));
  }

  @Test
  public void testCrossJoin() {
    givenQuery("SELECT * FROM catalog.employees CROSS JOIN catalog.departments")
        .assertPlan(
            """
            LogicalJoin(condition=[true], joinType=[inner])
              LogicalTableScan(table=[[catalog, employees]])
              LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testJoinWithFilterAndOrderBy() {
    givenQuery(
            """
            SELECT * FROM catalog.employees JOIN catalog.departments
              ON employees.department = departments.dept_name
              WHERE employees.age > 30 ORDER BY employees.name
            """)
        .assertPlan(
            """
            LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])
              LogicalFilter(condition=[>($2, 30)])
                LogicalJoin(condition=[=($3, $5)], joinType=[inner])
                  LogicalTableScan(table=[[catalog, employees]])
                  LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testUnionAll() {
    givenQuery(
            """
            SELECT name, age FROM catalog.employees
              UNION ALL SELECT dept_name, dept_id FROM catalog.departments
            """)
        .assertPlan(
            """
            LogicalUnion(all=[true])
              LogicalProject(name=[$1], age=[$2])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1], dept_id=[$0])
                LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testUnion() {
    givenQuery(
            """
            SELECT name, age FROM catalog.employees
              UNION SELECT dept_name, dept_id FROM catalog.departments
            """)
        .assertPlan(
            """
            LogicalUnion(all=[false])
              LogicalProject(name=[$1], age=[$2])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1], dept_id=[$0])
                LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testMultiWayUnion() {
    givenQuery(
            """
            SELECT name, age FROM catalog.employees
              UNION ALL SELECT dept_name, dept_id FROM catalog.departments
              UNION ALL SELECT name, age FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalUnion(all=[true])
              LogicalProject(name=[$1], age=[$2])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1], dept_id=[$0])
                LogicalTableScan(table=[[catalog, departments]])
              LogicalProject(name=[$1], age=[$2])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMinus() {
    givenQuery(
            """
            SELECT name, age FROM catalog.employees
              MINUS SELECT dept_name, dept_id FROM catalog.departments
            """)
        .assertPlan(
            """
            LogicalMinus(all=[false])
              LogicalProject(name=[$1], age=[$2])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1], dept_id=[$0])
                LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testInSubquery() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
              WHERE age IN (SELECT age FROM catalog.departments WHERE dept_name = 'Engineering')
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[IN($2, {
            LogicalProject(age=[$cor0.age])
              LogicalFilter(condition=[=($1, 'Engineering')])
                LogicalTableScan(table=[[catalog, departments]])
            })], variablesSet=[[$cor0]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testExistsSubquery() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
              WHERE EXISTS (SELECT 1 FROM catalog.departments WHERE dept_id = age)
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[EXISTS({
            LogicalProject(1=[1])
              LogicalFilter(condition=[=($0, $cor0.age)])
                LogicalTableScan(table=[[catalog, departments]])
            })], variablesSet=[[$cor0]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }
}
