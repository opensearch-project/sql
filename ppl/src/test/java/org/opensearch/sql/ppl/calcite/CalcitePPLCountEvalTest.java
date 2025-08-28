/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Unit tests for count(eval) functionality in CalcitePPL engine. Tests various scenarios of
 * filtered count aggregations.
 */
public class CalcitePPLCountEvalTest extends CalcitePPLAbstractTest {

  public CalcitePPLCountEvalTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testCountEvalSimpleCondition() {
    withPPLQuery("source=EMP | stats count(eval(SAL > 2000)) as c")
        .expectLogical(
            "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
                + "  LogicalProject($f1=[CASE(>($5, 2000), 1, null:NULL)])\n"
                + "    LogicalTableScan(table=[[scott, EMP]])\n")
        .expectResult("c=6\n")
        .expectSparkSQL(
            "SELECT COUNT(CASE WHEN `SAL` > 2000 THEN 1 ELSE NULL END) `c`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testCountEvalComplexCondition() {
    withPPLQuery("source=EMP | stats count(eval(SAL > 2000 and DEPTNO < 30)) as c")
        .expectLogical(
            "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
                + "  LogicalProject($f2=[CASE(AND(>($5, 2000), <($7, 30)), 1, null:NULL)])\n"
                + "    LogicalTableScan(table=[[scott, EMP]])\n")
        .expectResult("c=5\n")
        .expectSparkSQL(
            "SELECT COUNT(CASE WHEN `SAL` > 2000 AND `DEPTNO` < 30 THEN 1 ELSE NULL END) `c`\n"
                + "FROM `scott`.`EMP`");
  }

  @Test
  public void testCountEvalStringComparison() {
    withPPLQuery("source=EMP | stats count(eval(JOB = 'MANAGER')) as manager_count")
        .expectLogical(
            "LogicalAggregate(group=[{}], manager_count=[COUNT($0)])\n"
                + "  LogicalProject($f1=[CASE(=($2, 'MANAGER':VARCHAR), 1, null:NULL)])\n"
                + "    LogicalTableScan(table=[[scott, EMP]])\n")
        .expectResult("manager_count=3\n")
        .expectSparkSQL(
            "SELECT COUNT(CASE WHEN `JOB` = 'MANAGER' THEN 1 ELSE NULL END) `manager_count`\n"
                + "FROM `scott`.`EMP`");
  }

  @Test
  public void testCountEvalArithmeticExpression() {
    withPPLQuery("source=EMP | stats count(eval(SAL / COMM > 10)) as high_ratio")
        .expectLogical(
            "LogicalAggregate(group=[{}], high_ratio=[COUNT($0)])\n"
                + "  LogicalProject($f2=[CASE(>(DIVIDE($5, $6), 10), 1, null:NULL)])\n"
                + "    LogicalTableScan(table=[[scott, EMP]])\n")
        .expectResult("high_ratio=0\n")
        .expectSparkSQL(
            "SELECT COUNT(CASE WHEN `DIVIDE`(`SAL`, `COMM`) > 10 THEN 1 ELSE NULL END)"
                + " `high_ratio`\n"
                + "FROM `scott`.`EMP`");
  }

  @Test
  public void testCountEvalWithNullHandling() {
    withPPLQuery("source=EMP | stats count(eval(isnotnull(MGR))) as non_null_mgr")
        .expectLogical(
            "LogicalAggregate(group=[{}], non_null_mgr=[COUNT($0)])\n"
                + "  LogicalProject($f1=[CASE(IS NOT NULL($3), 1, null:NULL)])\n"
                + "    LogicalTableScan(table=[[scott, EMP]])\n")
        .expectResult("non_null_mgr=13\n")
        .expectSparkSQL(
            "SELECT COUNT(CASE WHEN `MGR` IS NOT NULL THEN 1 ELSE NULL END) `non_null_mgr`\n"
                + "FROM `scott`.`EMP`");
  }

  @Test
  public void testShortcutCEvalSimpleCondition() {
    withPPLQuery("source=EMP | stats c(eval(SAL > 2000)) as c")
        .expectLogical(
            "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
                + "  LogicalProject($f1=[CASE(>($5, 2000), 1, null:NULL)])\n"
                + "    LogicalTableScan(table=[[scott, EMP]])\n")
        .expectResult("c=6\n")
        .expectSparkSQL(
            "SELECT COUNT(CASE WHEN `SAL` > 2000 THEN 1 ELSE NULL END) `c`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testShortcutCEvalComplexCondition() {
    withPPLQuery("source=EMP | stats c(eval(JOB = 'MANAGER')) as manager_count")
        .expectLogical(
            "LogicalAggregate(group=[{}], manager_count=[COUNT($0)])\n"
                + "  LogicalProject($f1=[CASE(=($2, 'MANAGER':VARCHAR), 1, null:NULL)])\n"
                + "    LogicalTableScan(table=[[scott, EMP]])\n")
        .expectResult("manager_count=3\n")
        .expectSparkSQL(
            "SELECT COUNT(CASE WHEN `JOB` = 'MANAGER' THEN 1 ELSE NULL END) `manager_count`\n"
                + "FROM `scott`.`EMP`");
  }
}
