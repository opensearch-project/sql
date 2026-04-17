/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.ResultSetAssertion;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;

public class DatetimeUdtExtensionTest extends UnifiedQueryTestBase implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Before
  public void setUp() {
    super.setUp();
    compiler = new UnifiedQueryCompiler(context);
  }

  @Override
  protected Table createEmployeesTable() {
    return SimpleTable.builder()
        .col("name", SqlTypeName.VARCHAR)
        .col("hire_date", SqlTypeName.DATE)
        .col("login_time", SqlTypeName.TIME)
        .col("updated_at", SqlTypeName.TIMESTAMP)
        .row(
            new Object[] {
              "Alice",
              (int) LocalDate.of(2020, 3, 15).toEpochDay(),
              (int) (LocalTime.of(9, 30).toNanoOfDay() / 1_000_000),
              1705312200000L
            })
        .build();
  }

  private ResultSet execute(RelNode plan) throws Exception {
    PreparedStatement stmt = compiler.compile(plan);
    return stmt.executeQuery();
  }

  @Test
  public void castsStdDatetimeAsPplUdfOperand() throws Exception {
    RelNode plan =
        givenQuery("source = catalog.employees | eval y = YEAR(hire_date) | fields y")
            .assertPlan(
                """
                LogicalProject(y=[YEAR(CAST($1):EXPR_DATE VARCHAR NOT NULL)])
                  LogicalTableScan(table=[[catalog, employees]])
                """)
            .plan();
    verify(execute(plan)).expectSchema(col("y", INTEGER)).expectData(row(2020));
  }

  @Test
  public void castsStdDatetimeInUdtComparison() throws Exception {
    RelNode plan =
        givenQuery(
                "source = catalog.employees | where hire_date > DATE('2020-01-01') | fields name")
            .assertPlan(
                """
                LogicalProject(name=[$0])
                  LogicalFilter(condition=[>(CAST($1):EXPR_DATE VARCHAR NOT NULL, DATE('2020-01-01':VARCHAR))])
                    LogicalTableScan(table=[[catalog, employees]])
                """)
            .plan();
    verify(execute(plan)).expectSchema(col("name", VARCHAR)).expectData(row("Alice"));
  }

  @Test
  public void leavesUdtReturnTypeUntouched() throws Exception {
    RelNode plan =
        givenQuery("source = catalog.employees | eval d = LAST_DAY(hire_date) | fields d")
            .assertPlan(
                """
                LogicalProject(d=[LAST_DAY(CAST($1):EXPR_DATE VARCHAR NOT NULL)])
                  LogicalTableScan(table=[[catalog, employees]])
                """)
            .plan();
    verify(execute(plan)).expectSchema(col("d", VARCHAR)).expectData(row("2020-03-31"));
  }

  @Test
  public void castsBareStdDatetimeInProjection() throws Exception {
    RelNode plan =
        givenQuery("source = catalog.employees | fields hire_date, login_time")
            .assertPlan(
                """
                LogicalProject(hire_date=[CAST($1):EXPR_DATE VARCHAR NOT NULL], login_time=[CAST($2):EXPR_TIME VARCHAR NOT NULL])
                  LogicalTableScan(table=[[catalog, employees]])
                """)
            .plan();
    verify(execute(plan))
        .expectSchema(col("hire_date", VARCHAR), col("login_time", VARCHAR))
        .expectData(row("2020-03-15", "09:30:00"));
  }

  @Test
  public void leavesNonDatetimeUntouched() throws Exception {
    RelNode plan =
        givenQuery("source = catalog.employees | fields name")
            .assertPlan(
                """
                LogicalProject(name=[$0])
                  LogicalTableScan(table=[[catalog, employees]])
                """)
            .plan();
    verify(execute(plan)).expectSchema(col("name", VARCHAR)).expectData(row("Alice"));
  }
}
