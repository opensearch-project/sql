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
  public void testMultipleAggregatesWithAliasesByClause() {
    String ppl =
        "source=EMP | stats avg(SAL) as avg_sal, max(SAL) as max_sal, min(SAL) as min_sal, count()"
            + " as cnt by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{7}], avg_sal=[AVG($5)], max_sal=[MAX($5)], min_sal=[MIN($5)],"
            + " cnt=[COUNT()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "DEPTNO=20; avg_sal=2175.00; max_sal=3000.00; min_sal=800.00; cnt=5\n"
            + "DEPTNO=10; avg_sal=2916.66; max_sal=5000.00; min_sal=1300.00; cnt=3\n"
            + "DEPTNO=30; avg_sal=1566.66; max_sal=2850.00; min_sal=950.00; cnt=6\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, AVG(`SAL`) `avg_sal`, MAX(`SAL`) `max_sal`, MIN(`SAL`) `min_sal`,"
            + " COUNT(*) `cnt`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
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

  /**
   * TODO Calcite doesn't support group by window, but it support Tumble table function. See
   * `SqlToRelConverterTest`
   */
  @Test
  public void testAvgByTimeSpanAndFields() {
    String ppl =
        "source=EMP | stats avg(SAL) by span(HIREDATE, 1 day) as hiredate_span, DEPTNO | sort"
            + " DEPTNO, hiredate_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "  LogicalAggregate(group=[{1, 2}], avg(SAL)=[AVG($0)])\n"
            + "    LogicalProject(SAL=[$5], DEPTNO=[$7], hiredate_span=[86400000:INTERVAL DAY])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=10; hiredate_span=+1; avg(SAL)=2916.66\n"
            + "DEPTNO=20; hiredate_span=+1; avg(SAL)=2175.00\n"
            + "DEPTNO=30; hiredate_span=+1; avg(SAL)=1566.66\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `DEPTNO`, INTERVAL '1' DAY `hiredate_span`, AVG(`SAL`) `avg(SAL)`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, INTERVAL '1' DAY\n"
            + "ORDER BY `DEPTNO` NULLS LAST, INTERVAL '1' DAY NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
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

  @Test
  public void testCountDistinctWithAlias() {
    String ppl = "source=EMP | stats distinct_count(JOB) as dc by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{7}], dc=[COUNT(DISTINCT $2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "" + "DEPTNO=20; dc=3\n" + "DEPTNO=10; dc=3\n" + "DEPTNO=30; dc=3\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `DEPTNO`, COUNT(DISTINCT `JOB`) `dc`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Ignore
  public void testApproxCountDistinct() {
    String ppl = "source=EMP | stats distinct_count_approx(JOB) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{7}], distinct_count_approx(JOB)=[COUNT(DISTINCT $2)])\n"
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

  @Test
  public void testStddevSampByField() {
    String ppl = "source=EMP | stats stddev_samp(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{7}], stddev_samp(SAL)=[STDDEV_SAMP($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "DEPTNO=20; stddev_samp(SAL)=1123.33\n"
            + "DEPTNO=10; stddev_samp(SAL)=1893.62\n"
            + "DEPTNO=30; stddev_samp(SAL)=668.33\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `DEPTNO`, STDDEV_SAMP(`SAL`) `stddev_samp(SAL)`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStddevSampByFieldWithAlias() {
    String ppl = "source=EMP | stats stddev_samp(SAL) as samp by span(EMPNO, 100) as empno_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{1}], samp=[STDDEV_SAMP($0)])\n"
            + "  LogicalProject(SAL=[$5], empno_span=[*(FLOOR(/($0, 100)), 100)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "empno_span=7300.0; samp=null\n"
            + "empno_span=7400.0; samp=null\n"
            + "empno_span=7500.0; samp=1219.75\n"
            + "empno_span=7600.0; samp=1131.37\n"
            + "empno_span=7700.0; samp=388.90\n"
            + "empno_span=7800.0; samp=2145.53\n"
            + "empno_span=7900.0; samp=1096.58\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT FLOOR(`EMPNO` / 100) * 100 `empno_span`, STDDEV_SAMP(`SAL`) `samp`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY FLOOR(`EMPNO` / 100) * 100";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStddevPopByField() {
    String ppl = "source=EMP | stats stddev_pop(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{7}], stddev_pop(SAL)=[STDDEV_POP($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "DEPTNO=20; stddev_pop(SAL)=1004.73\n"
            + "DEPTNO=10; stddev_pop(SAL)=1546.14\n"
            + "DEPTNO=30; stddev_pop(SAL)=610.10\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `DEPTNO`, STDDEV_POP(`SAL`) `stddev_pop(SAL)`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStddevPopByFieldWithAlias() {
    String ppl = "source=EMP | stats stddev_pop(SAL) as pop by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{7}], pop=[STDDEV_POP($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "" + "DEPTNO=20; pop=1004.73\n" + "DEPTNO=10; pop=1546.14\n" + "DEPTNO=30; pop=610.10\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `DEPTNO`, STDDEV_POP(`SAL`) `pop`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAggWithEval() {
    String ppl = "source=EMP | eval a = 1 | stats avg(a) as avg_a";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], avg_a=[AVG($0)])\n"
            + "  LogicalProject(a=[1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg_a=1.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT AVG(1) `avg_a`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAggByWithEval() {
    String ppl = "source=EMP | eval a = 1, b = a | stats avg(a) as avg_a by b";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{1}], avg_a=[AVG($0)])\n"
            + "  LogicalProject(a=[1], b=[1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "b=1; avg_a=1.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT 1 `b`, AVG(1) `avg_a`\n" + "FROM `scott`.`EMP`\n" + "GROUP BY 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAggWithBackticksAlias() {
    String ppl = "source=EMP | stats avg(`SAL`) as `avg_sal`";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], avg_sal=[AVG($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg_sal=2073.21\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT AVG(`SAL`) `avg_sal`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSimpleTwoLevelStats() {
    String ppl = "source=EMP | stats avg(SAL) as avg_sal | stats avg(avg_sal) as avg_avg_sal";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], avg_avg_sal=[AVG($0)])\n"
            + "  LogicalAggregate(group=[{}], avg_sal=[AVG($5)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "avg_avg_sal=2073.21\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`avg_sal`) `avg_avg_sal`\n"
            + "FROM (SELECT AVG(`SAL`) `avg_sal`\n"
            + "FROM `scott`.`EMP`) `t`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTwoLevelStats() {
    String ppl =
        "source=EMP | stats avg(SAL) as avg_sal by DEPTNO, MGR | stats avg(avg_sal) as avg_avg_sal"
            + " by MGR";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{0}], avg_avg_sal=[AVG($2)])\n"
            + "  LogicalAggregate(group=[{3, 7}], avg_sal=[AVG($5)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "MGR=null; avg_avg_sal=5000.00\n"
            + "MGR=7698; avg_avg_sal=1310.00\n"
            + "MGR=7782; avg_avg_sal=1300.00\n"
            + "MGR=7788; avg_avg_sal=1100.00\n"
            + "MGR=7902; avg_avg_sal=800.00\n"
            + "MGR=7566; avg_avg_sal=3000.00\n"
            + "MGR=7839; avg_avg_sal=2758.33\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `MGR`, AVG(`avg_sal`) `avg_avg_sal`\n"
            + "FROM (SELECT `MGR`, `DEPTNO`, AVG(`SAL`) `avg_sal`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `MGR`, `DEPTNO`) `t`\n"
            + "GROUP BY `MGR`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
