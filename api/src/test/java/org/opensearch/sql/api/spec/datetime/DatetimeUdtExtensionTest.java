/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static java.sql.Types.BIGINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertFalse;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.ResultSetAssertion;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.api.spec.datetime.DatetimeUdtExtension.UdtMapping;

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
            }) // 2024-01-15 10:30:00 UTC
        .build();
  }

  private void assertNoUdtInRexCalls(RelNode plan) {
    plan.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            other.accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitCall(RexCall call) {
                    assertFalse(
                        "RexCall " + call + " has UDT return type: " + call.getType(),
                        UdtMapping.fromUdtType(call.getType()).isPresent());
                    return super.visitCall(call);
                  }
                });
            return super.visit(other);
          }
        });
  }

  private ResultSet planAndExecute(String query) throws Exception {
    RelNode plan = planner.plan(query);
    assertNoUdtInRexCalls(plan);
    PreparedStatement stmt = compiler.compile(plan);
    return stmt.executeQuery();
  }

  @Test
  public void testDateUdtNormalization() throws Exception {
    ResultSet rs =
        planAndExecute(
            "source = catalog.employees"
                + " | eval last = LAST_DAY(hire_date), y = YEAR(hire_date)"
                + " | fields last, y");
    verify(rs)
        .expectSchema(col("last", VARCHAR), col("y", INTEGER))
        .expectData(row("2020-03-31", 2020));
  }

  @Test
  public void testTimeUdtNormalization() throws Exception {
    ResultSet rs =
        planAndExecute(
            "source = catalog.employees | where login_time > MAKETIME(8, 0, 0) | fields"
                + " login_time");
    verify(rs).expectSchema(col("login_time", VARCHAR)).expectData(row("09:30:00"));
  }

  @Test
  public void testTimestampUdtNormalization() throws Exception {
    ResultSet rs =
        planAndExecute(
            "source = catalog.employees"
                + " | where updated_at > TIMESTAMP('2024-01-01 00:00:00')"
                + " | eval fmt = DATE_FORMAT(updated_at, '%Y-%m')"
                + " | fields fmt");
    verify(rs).expectSchema(col("fmt", VARCHAR)).expectData(row("2024-01"));
  }

  @Test
  public void testNestedUdfCalls() throws Exception {
    ResultSet rs =
        planAndExecute(
            "source = catalog.employees"
                + " | eval days = DATEDIFF(DATE('2025-01-01'), DATE(hire_date))"
                + " | fields days");
    verify(rs).expectSchema(col("days", BIGINT)).expectData(row(1753L));
  }

  @Test
  public void testUnrelatedUdfUnaffected() throws Exception {
    ResultSet rs =
        planAndExecute("source = catalog.employees | eval s = CONCAT(name, ' test') | fields s");
    verify(rs).expectSchema(col("s", VARCHAR)).expectData(row("Alice test"));
  }

  @Test
  public void testBareDatetimeColumnPassthrough() throws Exception {
    // DATE and TIME are TZ-independent; TIMESTAMP formatting depends on JVM TZ
    ResultSet rs = planAndExecute("source = catalog.employees | fields hire_date, login_time");
    verify(rs)
        .expectSchema(col("hire_date", VARCHAR), col("login_time", VARCHAR))
        .expectData(row("2020-03-15", "09:30:00"));
  }

  @Test
  public void testStandardCalciteFunctionUnaffected() throws Exception {
    ResultSet rs = planAndExecute("source = catalog.employees | eval u = UPPER(name) | fields u");
    verify(rs).expectSchema(col("u", VARCHAR)).expectData(row("ALICE"));
  }
}
