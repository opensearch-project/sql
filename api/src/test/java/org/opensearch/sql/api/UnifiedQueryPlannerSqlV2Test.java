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
 * and JOIN operations that produce valid RelNode plans.
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

  @Test
  public void testNotInSubquery() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
              WHERE age NOT IN (SELECT age FROM catalog.departments WHERE dept_name = 'Engineering')
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[NOT(IN($2, {
            LogicalProject(age=[$cor0.age])
              LogicalFilter(condition=[=($1, 'Engineering')])
                LogicalTableScan(table=[[catalog, departments]])
            }))], variablesSet=[[$cor0]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testUnionAll() {
    givenQuery(
            """
            SELECT name FROM catalog.employees UNION ALL SELECT dept_name FROM catalog.departments
            """)
        .assertPlan(
            """
            LogicalUnion(all=[true])
              LogicalProject(name=[$1])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1])
                LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testMultiWayUnion() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
            UNION ALL SELECT dept_name FROM catalog.departments
            UNION ALL SELECT name FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalUnion(all=[true])
              LogicalProject(name=[$1])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1])
                LogicalTableScan(table=[[catalog, departments]])
              LogicalProject(name=[$1])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testNotExistsSubquery() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
              WHERE NOT EXISTS (SELECT 1 FROM catalog.departments WHERE dept_id = age)
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[NOT(EXISTS({
            LogicalProject(1=[1])
              LogicalFilter(condition=[=($0, $cor0.age)])
                LogicalTableScan(table=[[catalog, departments]])
            }))], variablesSet=[[$cor0]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void selectLiteralWithoutFrom() {
    // FROM-less SELECT produces a one-row result via LogicalValues so the downstream
    // Project evaluates over a single row.
    givenQuery("SELECT 1")
        .assertPlan(
            """
            LogicalSort(sort0=[$0], dir0=[ASC])
              LogicalValues(tuples=[[{ 1 }]])
            """);
  }

  @Test
  public void selectExpressionWithoutFrom() {
    givenQuery("SELECT 1 + 1")
        .assertPlan(
            """
            LogicalProject(1 + 1=[+(1, 1)])
              LogicalValues(tuples=[[{ 0 }]])
            """);
  }

  @Test
  public void testHavingMaxCol() {
    givenQuery(
            """
            SELECT department FROM catalog.employees
              GROUP BY department HAVING MAX(age) > 30
            """)
        .assertPlan(
            """
            LogicalProject(department=[$1])
              LogicalFilter(condition=[>($0, 30)])
                LogicalProject(MAX(age)=[$1], department=[$0])
                  LogicalAggregate(group=[{0}], MAX(age)=[MAX($1)])
                    LogicalProject(department=[$3], age=[$2])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testScalarFnOverAggregate() {
    givenQuery("SELECT ABS(MAX(age)) FROM catalog.employees")
        .assertPlan(
            """
            LogicalProject(ABS(MAX(age))=[ABS($0)])
              LogicalAggregate(group=[{}], MAX(age)=[MAX($0)])
                LogicalProject(age=[$2])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testArithmeticOnAggregates() {
    givenQuery("SELECT MAX(age) + MIN(age) AS range_sum FROM catalog.employees")
        .assertPlan(
            """
            LogicalProject(range_sum=[+($0, $1)])
              LogicalAggregate(group=[{}], MAX(age)=[MAX($0)], MIN(age)=[MIN($0)])
                LogicalProject(age=[$2])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testHavingCountStar() {
    givenQuery(
            """
            SELECT department FROM catalog.employees
              GROUP BY department HAVING COUNT(*) > 5
            """)
        .assertPlan(
            """
            LogicalProject(department=[$1])
              LogicalFilter(condition=[>($0, 5)])
                LogicalProject(COUNT(*)=[$1], department=[$0])
                  LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                    LogicalProject(department=[$3])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testHavingWithAlias() {
    givenQuery(
            """
            SELECT department, COUNT(*) AS cnt FROM catalog.employees
              GROUP BY department HAVING cnt > 1
            """)
        .assertPlan(
            """
            LogicalProject(department=[$1], cnt=[$0])
              LogicalFilter(condition=[>($0, 1)])
                LogicalProject(COUNT(*)=[$1], department=[$0])
                  LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                    LogicalProject(department=[$3])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testHavingCompoundAnd() {
    givenQuery(
            """
            SELECT department FROM catalog.employees
              GROUP BY department HAVING MAX(age) > 30 AND MIN(age) < 50
            """)
        .assertPlan(
            """
            LogicalProject(department=[$2])
              LogicalFilter(condition=[AND(>($0, 30), <($1, 50))])
                LogicalProject(MAX(age)=[$1], MIN(age)=[$2], department=[$0])
                  LogicalAggregate(group=[{0}], MAX(age)=[MAX($1)], MIN(age)=[MIN($1)])
                    LogicalProject(department=[$3], age=[$2])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testWindowOrderByDefaultsNullsFirst() {
    // Window function ORDER BY without explicit NULLS FIRST/LAST defaults to NULLS FIRST,
    // matching top-level ORDER BY semantics.
    givenQuery(
            """
            SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1], rn=[ROW_NUMBER() OVER (ORDER BY $0 NULLS FIRST)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }
}
