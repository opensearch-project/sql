/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLCaseFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLCaseFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testCaseWhen() {
    String ppl =
        "source=EMP | eval a = case(DEPTNO >= 20 AND DEPTNO < 30, 'A', DEPTNO >= 30 AND DEPTNO <"
            + " 40, 'B', DEPTNO >= 40 AND DEPTNO < 50, 'C', DEPTNO >= 50 AND DEPTNO < 60, 'D' else"
            + " 'E') | fields a";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(a=[CASE(SEARCH($7, Sarg[[20..30)]), 'A', SEARCH($7, Sarg[[30..40)]), 'B',"
            + " SEARCH($7, Sarg[[40..50)]), 'C', SEARCH($7, Sarg[[50..60)]), 'D', 'E')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT CASE WHEN `DEPTNO` >= 20 AND `DEPTNO` < 30 THEN 'A' WHEN `DEPTNO` >= 30 AND"
            + " `DEPTNO` < 40 THEN 'B' WHEN `DEPTNO` >= 40 AND `DEPTNO` < 50 THEN 'C' WHEN `DEPTNO`"
            + " >= 50 AND `DEPTNO` < 60 THEN 'D' ELSE 'E' END `a`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCaseWhenInFilter() {
    String ppl =
        "source=EMP | where not true = case(DEPTNO in (20, 21), true, DEPTNO in (30, 31), false,"
            + " DEPTNO in (40, 41), true else false)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[AND(IS NOT TRUE(SEARCH($7, Sarg[20, 21])), OR(IS NOT"
            + " TRUE(SEARCH($7, Sarg[40, 41])), SEARCH($7, Sarg[30, 31])))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IN (20, 21) IS NOT TRUE AND (`DEPTNO` IN (40, 41) IS NOT TRUE OR"
            + " `DEPTNO` IN (30, 31))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCaseWhenInSubquery() {
    String ppl =
        "source=EMP | where DEPTNO in [source=EMP | eval new_deptno = case(DEPTNO in (20, 21), 20,"
            + " DEPTNO in (30, 31), 30 else 100) | fields new_deptno] | fields DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[IN($7, {\n"
            + "LogicalProject(new_deptno=[CASE(SEARCH($7, Sarg[20, 21]), 20, SEARCH($7, Sarg[30,"
            + " 31]), 30, 100)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IN (SELECT CASE WHEN `DEPTNO` IN (20, 21) THEN 20 WHEN `DEPTNO` IN"
            + " (30, 31) THEN 30 ELSE 100 END `new_deptno`\n"
            + "FROM `scott`.`EMP`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
