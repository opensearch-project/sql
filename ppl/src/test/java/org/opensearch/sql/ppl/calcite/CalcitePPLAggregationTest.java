/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Ignore;
import org.junit.Test;

public class CalcitePPLAggregationTest extends CalcitePPLAbstractTest {

  public CalcitePPLAggregationTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testSimpleCount() {
    String ppl = "source=EMP | stats count() as c";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "c=14\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT COUNT(*) `c`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSimpleAvg() {
    String ppl = "source=EMP | stats avg(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], avg(SAL)=[AVG($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg(SAL)=2073.21\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT AVG(`SAL`) `avg(SAL)`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleAggregatesWithAliases() {
    String ppl =
        "source=EMP | stats avg(SAL) as avg_sal, max(SAL) as max_sal, min(SAL) as min_sal, count()"
            + " as cnt";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], avg_sal=[AVG($5)], max_sal=[MAX($5)], min_sal=[MIN($5)],"
            + " cnt=[COUNT()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg_sal=2073.21; max_sal=5000.00; min_sal=800.00; cnt=14\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT AVG(`SAL`) `avg_sal`, MAX(`SAL`) `max_sal`, MIN(`SAL`) `min_sal`, COUNT(*) `cnt`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAvgByField() {
    String ppl = "source=EMP | stats avg(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{7}], avg(SAL)=[AVG($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "DEPTNO=20; avg(SAL)=2175.00\n"
            + "DEPTNO=10; avg(SAL)=2916.66\n"
            + "DEPTNO=30; avg(SAL)=1566.66\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `DEPTNO`, AVG(`SAL`) `avg(SAL)`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAvgBySpan() {
    String ppl = "source=EMP | stats avg(SAL) by span(EMPNO, 100) as empno_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{1}], avg(SAL)=[AVG($0)])\n"
            + "  LogicalProject(SAL=[$5], empno_span=[*(FLOOR(/($0, 100)), 100)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "empno_span=7300.0; avg(SAL)=800.00\n"
            + "empno_span=7400.0; avg(SAL)=1600.00\n"
            + "empno_span=7500.0; avg(SAL)=2112.50\n"
            + "empno_span=7600.0; avg(SAL)=2050.00\n"
            + "empno_span=7700.0; avg(SAL)=2725.00\n"
            + "empno_span=7800.0; avg(SAL)=2533.33\n"
            + "empno_span=7900.0; avg(SAL)=1750.00\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testAvgBySpanAndFields() {
    String ppl =
        "source=EMP | stats avg(SAL) by span(EMPNO, 500) as empno_span, DEPTNO | sort DEPTNO,"
            + " empno_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "  LogicalAggregate(group=[{1, 2}], avg(SAL)=[AVG($0)])\n"
            + "    LogicalProject(SAL=[$5], DEPTNO=[$7], empno_span=[*(FLOOR(/($0, 500)), 500)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "DEPTNO=10; empno_span=7500.0; avg(SAL)=2916.66\n"
            + "DEPTNO=20; empno_span=7000.0; avg(SAL)=800.00\n"
            + "DEPTNO=20; empno_span=7500.0; avg(SAL)=2518.75\n"
            + "DEPTNO=30; empno_span=7000.0; avg(SAL)=1600.00\n"
            + "DEPTNO=30; empno_span=7500.0; avg(SAL)=1560.00\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `DEPTNO`, FLOOR(`EMPNO` / 500) * 500 `empno_span`, AVG(`SAL`) `avg(SAL)`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, FLOOR(`EMPNO` / 500) * 500\n"
            + "ORDER BY `DEPTNO` NULLS LAST, 2 NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Ignore
  public void testAvgByTimeSpanAndFields() {
    String ppl =
        "source=EMP | stats avg(SAL) by span(HIREDATE, 1y) as hiredate_span, DEPTNO | sort DEPTNO,"
            + " hiredate_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical = "";
    verifyLogical(root, expectedLogical);
    String expectedResult = "";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testCountDistinct() {
    String ppl = "source=EMP | stats distinct_count(JOB) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{7}], distinct_count(JOB)=[COUNT(DISTINCT $2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "DEPTNO=20; distinct_count(JOB)=3\n"
            + "DEPTNO=10; distinct_count(JOB)=3\n"
            + "DEPTNO=30; distinct_count(JOB)=3\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `DEPTNO`, COUNT(DISTINCT `JOB`) `distinct_count(JOB)`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Ignore
  public void testMultipleLevelStats() {
    // TODO unsupported
    String ppl = "source=EMP | stats avg(SAL) as avg_sal | stats avg(COMM) as avg_comm";
    RelNode root = getRelNode(ppl);
    String expectedLogical = "";
    verifyLogical(root, expectedLogical);
    String expectedResult = "";
    verifyResult(root, expectedResult);
  }
}
