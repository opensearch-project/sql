/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Notice: PPL aggregation will add sort by default, for example:
 *
 * <p>{@code | stats count() by deptno,job} equals to
 *
 * <p>{@code GROUP BY deptno,job ORDER BY deptno,job}
 */
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
  public void testTakeAgg() {
    String ppl = "source=EMP | stats take(JOB, 2) as c";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[TAKE($0, $1)])\n"
            + "  LogicalProject(JOB=[$2], $f8=[2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "c=[CLERK, SALESMAN]\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT `TAKE`(`JOB`, 2) `c`\nFROM `scott`.`EMP`";
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
        ""
            + "LogicalProject(avg_sal=[$1], max_sal=[$2], min_sal=[$3], cnt=[$4], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{7}], avg_sal=[AVG($5)], max_sal=[MAX($5)],"
            + " min_sal=[MIN($5)], cnt=[COUNT()])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg_sal=2916.66; max_sal=5000.00; min_sal=1300.00; cnt=3; DEPTNO=10\n"
            + "avg_sal=2175.00; max_sal=3000.00; min_sal=800.00; cnt=5; DEPTNO=20\n"
            + "avg_sal=1566.66; max_sal=2850.00; min_sal=950.00; cnt=6; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT AVG(`SAL`) `avg_sal`, MAX(`SAL`) `max_sal`, MIN(`SAL`) `min_sal`,"
            + " COUNT(*) `cnt`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAvgByField() {
    String ppl = "source=EMP | stats avg(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(avg(SAL)=[$1], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{7}], avg(SAL)=[AVG($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(SAL)=2916.66; DEPTNO=10\n"
            + "avg(SAL)=2175.00; DEPTNO=20\n"
            + "avg(SAL)=1566.66; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`SAL`) `avg(SAL)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAvgByMultipleFieldsWithDiffOrder() {
    String ppl1 = "source=EMP | stats avg(SAL) by JOB, DEPTNO";
    RelNode root1 = getRelNode(ppl1);
    String expectedLogical1 =
        ""
            + "LogicalProject(avg(SAL)=[$2], JOB=[$0], DEPTNO=[$1])\n"
            + "  LogicalSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "    LogicalAggregate(group=[{2, 7}], avg(SAL)=[AVG($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root1, expectedLogical1);

    String expectedSparkSql1 =
        ""
            + "SELECT AVG(`SAL`) `avg(SAL)`, `JOB`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`, `DEPTNO`\n"
            + "ORDER BY `JOB` NULLS LAST, `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root1, expectedSparkSql1);

    String ppl2 = "source=EMP | stats avg(SAL) by DEPTNO, JOB";
    RelNode root2 = getRelNode(ppl2);
    String expectedLogical2 =
        ""
            + "LogicalProject(avg(SAL)=[$2], DEPTNO=[$1], JOB=[$0])\n"
            + "  LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])\n"
            + "    LogicalAggregate(group=[{2, 7}], avg(SAL)=[AVG($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root2, expectedLogical2);

    String expectedSparkSql2 =
        ""
            + "SELECT AVG(`SAL`) `avg(SAL)`, `DEPTNO`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`, `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST, `JOB` NULLS LAST";
    verifyPPLToSparkSQL(root2, expectedSparkSql2);
  }

  @Test
  public void testAvgBySpan() {
    String ppl = "source=EMP | stats avg(SAL) by span(EMPNO, 100) as empno_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(avg(SAL)=[$1], empno_span=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{1}], avg(SAL)=[AVG($0)])\n"
            + "      LogicalProject(SAL=[$5], empno_span=[*(FLOOR(/($0, 100)), 100)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(SAL)=800.00; empno_span=7300\n"
            + "avg(SAL)=1600.00; empno_span=7400\n"
            + "avg(SAL)=2112.50; empno_span=7500\n"
            + "avg(SAL)=2050.00; empno_span=7600\n"
            + "avg(SAL)=2725.00; empno_span=7700\n"
            + "avg(SAL)=2533.33; empno_span=7800\n"
            + "avg(SAL)=1750.00; empno_span=7900\n";
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
            + "LogicalSort(sort0=[$2], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "  LogicalProject(avg(SAL)=[$2], empno_span=[$1], DEPTNO=[$0])\n"
            + "    LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])\n"
            + "      LogicalAggregate(group=[{1, 2}], avg(SAL)=[AVG($0)])\n"
            + "        LogicalProject(SAL=[$5], DEPTNO=[$7], empno_span=[*(FLOOR(/($0, 500)),"
            + " 500)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(SAL)=2916.66; empno_span=7500; DEPTNO=10\n"
            + "avg(SAL)=800.00; empno_span=7000; DEPTNO=20\n"
            + "avg(SAL)=2518.75; empno_span=7500; DEPTNO=20\n"
            + "avg(SAL)=1600.00; empno_span=7000; DEPTNO=30\n"
            + "avg(SAL)=1560.00; empno_span=7500; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `avg(SAL)`, `empno_span`, `DEPTNO`\n"
            + "FROM (SELECT `DEPTNO`, FLOOR(`EMPNO` / 500) * 500 `empno_span`, AVG(`SAL`)"
            + " `avg(SAL)`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, FLOOR(`EMPNO` / 500) * 500\n"
            + "ORDER BY 2 NULLS LAST, `DEPTNO` NULLS LAST) `t1`\n"
            + "ORDER BY `DEPTNO` NULLS LAST, `empno_span` NULLS LAST";
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
            + "LogicalSort(sort0=[$2], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
            + "  LogicalProject(avg(SAL)=[$2], hiredate_span=[$1], DEPTNO=[$0])\n"
            + "    LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])\n"
            + "      LogicalAggregate(group=[{1, 2}], avg(SAL)=[AVG($0)])\n"
            + "        LogicalProject(SAL=[$5], DEPTNO=[$7], hiredate_span=[86400000:INTERVAL"
            + " DAY])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "avg(SAL)=2916.66; hiredate_span=+1; DEPTNO=10\n"
            + "avg(SAL)=2175.00; hiredate_span=+1; DEPTNO=20\n"
            + "avg(SAL)=1566.66; hiredate_span=+1; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM (SELECT AVG(`SAL`) `avg(SAL)`, INTERVAL '1' DAY `hiredate_span`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, INTERVAL '1' DAY\n"
            + "ORDER BY INTERVAL '1' DAY NULLS LAST, `DEPTNO` NULLS LAST) `t2`\n"
            + "ORDER BY `DEPTNO` NULLS LAST, `hiredate_span` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCountDistinct() {
    String ppl = "source=EMP | stats distinct_count(JOB) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(distinct_count(JOB)=[$1], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{7}], distinct_count(JOB)=[COUNT(DISTINCT $2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "distinct_count(JOB)=3; DEPTNO=10\n"
            + "distinct_count(JOB)=3; DEPTNO=20\n"
            + "distinct_count(JOB)=3; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT COUNT(DISTINCT `JOB`) `distinct_count(JOB)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCountDistinctWithAlias() {
    String ppl = "source=EMP | stats distinct_count(JOB) as dc by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(dc=[$1], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{7}], dc=[COUNT(DISTINCT $2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "dc=3; DEPTNO=10\ndc=3; DEPTNO=20\ndc=3; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT COUNT(DISTINCT `JOB`) `dc`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
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
            + "LogicalProject(stddev_samp(SAL)=[$1], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{7}], stddev_samp(SAL)=[STDDEV_SAMP($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "stddev_samp(SAL)=1893.62; DEPTNO=10\n"
            + "stddev_samp(SAL)=1123.33; DEPTNO=20\n"
            + "stddev_samp(SAL)=668.33; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT STDDEV_SAMP(`SAL`) `stddev_samp(SAL)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStddevSampByFieldWithAlias() {
    String ppl = "source=EMP | stats stddev_samp(SAL) as samp by span(EMPNO, 100) as empno_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(samp=[$1], empno_span=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{1}], samp=[STDDEV_SAMP($0)])\n"
            + "      LogicalProject(SAL=[$5], empno_span=[*(FLOOR(/($0, 100)), 100)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "samp=null; empno_span=7300\n"
            + "samp=null; empno_span=7400\n"
            + "samp=1219.75; empno_span=7500\n"
            + "samp=1131.37; empno_span=7600\n"
            + "samp=388.90; empno_span=7700\n"
            + "samp=2145.53; empno_span=7800\n"
            + "samp=1096.58; empno_span=7900\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT `samp`, `empno_span`\n"
            + "FROM (SELECT FLOOR(`EMPNO` / 100) * 100 `empno_span`, STDDEV_SAMP(`SAL`) `samp`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY FLOOR(`EMPNO` / 100) * 100\nORDER BY 1 NULLS LAST) `t1`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStddevPopByField() {
    String ppl = "source=EMP | stats stddev_pop(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(stddev_pop(SAL)=[$1], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{7}], stddev_pop(SAL)=[STDDEV_POP($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "stddev_pop(SAL)=1546.14; DEPTNO=10\n"
            + "stddev_pop(SAL)=1004.73; DEPTNO=20\n"
            + "stddev_pop(SAL)=610.10; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT STDDEV_POP(`SAL`) `stddev_pop(SAL)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStddevPopByFieldWithAlias() {
    String ppl = "source=EMP | stats stddev_pop(SAL) as pop by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(pop=[$1], DEPTNO=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{7}], pop=[STDDEV_POP($5)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "pop=1546.14; DEPTNO=10\npop=1004.73; DEPTNO=20\npop=610.10; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT STDDEV_POP(`SAL`) `pop`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST";
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
            + "LogicalProject(avg_a=[$1], b=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{1}], avg_a=[AVG($0)])\n"
            + "      LogicalProject(a=[1], b=[1])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg_a=1.0; b=1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(1) `avg_a`, 1 `b`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY 1\n"
            + "ORDER BY '1' NULLS LAST";
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
            + "LogicalProject(avg_avg_sal=[$1], MGR=[$0])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "    LogicalAggregate(group=[{1}], avg_avg_sal=[AVG($0)])\n"
            + "      LogicalProject(avg_sal=[$2], MGR=[$0])\n"
            + "        LogicalSort(sort0=[$1], sort1=[$0], dir0=[ASC], dir1=[ASC])\n"
            + "          LogicalAggregate(group=[{3, 7}], avg_sal=[AVG($5)])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "avg_avg_sal=3000.00; MGR=7566\n"
            + "avg_avg_sal=1310.00; MGR=7698\n"
            + "avg_avg_sal=1300.00; MGR=7782\n"
            + "avg_avg_sal=1100.00; MGR=7788\n"
            + "avg_avg_sal=2758.33; MGR=7839\n"
            + "avg_avg_sal=800.00; MGR=7902\n"
            + "avg_avg_sal=5000.00; MGR=null\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`avg_sal`) `avg_avg_sal`, `MGR`\n"
            + "FROM (SELECT AVG(`SAL`) `avg_sal`, `MGR`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `MGR`, `DEPTNO`\n"
            + "ORDER BY `DEPTNO` NULLS LAST, `MGR` NULLS LAST) `t1`\n"
            + "GROUP BY `MGR`\n"
            + "ORDER BY `MGR` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
