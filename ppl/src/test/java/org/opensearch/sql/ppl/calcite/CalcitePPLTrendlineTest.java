/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert.SchemaSpec;
import org.junit.Test;

public class CalcitePPLTrendlineTest extends CalcitePPLAbstractTest {
  public CalcitePPLTrendlineTest() {
    super(SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testTrendlineSma() {
    String ppl = "source=EMP | trendline sma(2, SAL) as sal_trend | fields SAL, sal_trend";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(SAL=[$5], sal_trend=[CASE(>(COUNT() OVER (ROWS 1 PRECEDING), 1), /(SUM($5)"
            + " OVER (ROWS 1 PRECEDING), CAST(COUNT($5) OVER (ROWS 1 PRECEDING)):DOUBLE NOT NULL),"
            + " null:NULL)])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($5)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `SAL`, CASE WHEN (COUNT(*) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) > 1"
            + " THEN (SUM(`SAL`) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) /"
            + " CAST(COUNT(`SAL`) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS DOUBLE) ELSE"
            + " NULL END `sal_trend`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` IS NOT NULL";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTrendlineWma() {
    String ppl = "source=EMP | trendline wma(3, SAL) | fields SAL, SAL_trendline";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(SAL=[$5], SAL_trendline=[CASE(>(COUNT() OVER (ROWS 2 PRECEDING), 2),"
            + " /(+(+(CAST(CAST(NTH_VALUE($5, 1) OVER (ROWS 2 PRECEDING)):DECIMAL(17,"
            + " 2)):DECIMAL(18, 2), *(NTH_VALUE($5, 2) OVER (ROWS 2 PRECEDING), 2)),"
            + " *(NTH_VALUE($5, 3) OVER (ROWS 2 PRECEDING), 3)), 6.0E0:DOUBLE), null:NULL)])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($5)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `SAL`, CASE WHEN (COUNT(*) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) > 2"
            + " THEN (CAST(CAST(NTH_VALUE(`SAL`, 1) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"
            + " AS DECIMAL(17, 2)) AS DECIMAL(18, 2)) + (NTH_VALUE(`SAL`, 2) OVER (ROWS BETWEEN 2"
            + " PRECEDING AND CURRENT ROW)) * 2 + (NTH_VALUE(`SAL`, 3) OVER (ROWS BETWEEN 2"
            + " PRECEDING AND CURRENT ROW)) * 3) / 6.0E0 ELSE NULL END `SAL_trendline`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` IS NOT NULL";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTrendlineMultipleFields() {
    String ppl =
        "source=EMP | trendline wma(2, SAL) sma(2, DEPTNO) | fields SAL_trendline,"
            + " DEPTNO_trendline";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(SAL_trendline=[CASE(>(COUNT() OVER (ROWS 1 PRECEDING), 1),"
            + " /(+(CAST(CAST(NTH_VALUE($5, 1) OVER (ROWS 1 PRECEDING)):DECIMAL(17, 2)):DECIMAL(18,"
            + " 2), *(NTH_VALUE($5, 2) OVER (ROWS 1 PRECEDING), 2)), 3.0E0:DOUBLE), null:NULL)],"
            + " DEPTNO_trendline=[CASE(>(COUNT() OVER (ROWS 1 PRECEDING), 1), /(SUM($7) OVER (ROWS"
            + " 1 PRECEDING), CAST(COUNT($7) OVER (ROWS 1 PRECEDING)):DOUBLE NOT NULL),"
            + " null:NULL)])\n"
            + "  LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "    LogicalFilter(condition=[IS NOT NULL($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT CASE WHEN (COUNT(*) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) > 1 THEN"
            + " (CAST(CAST(NTH_VALUE(`SAL`, 1) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS"
            + " DECIMAL(17, 2)) AS DECIMAL(18, 2)) + (NTH_VALUE(`SAL`, 2) OVER (ROWS BETWEEN 1"
            + " PRECEDING AND CURRENT ROW)) * 2) / 3.0E0 ELSE NULL END `SAL_trendline`, CASE WHEN"
            + " (COUNT(*) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) > 1 THEN (SUM(`DEPTNO`)"
            + " OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)) / CAST(COUNT(`DEPTNO`) OVER (ROWS"
            + " BETWEEN 1 PRECEDING AND CURRENT ROW) AS DOUBLE) ELSE NULL END `DEPTNO_trendline`\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` IS NOT NULL) `t`\n"
            + "WHERE `DEPTNO` IS NOT NULL";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
