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

    ppl = "source=EMP | stats c() as count_emp";
    root = getRelNode(ppl);
    expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], count_emp=[COUNT()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "count_emp=14\n";
    verifyResult(root, expectedResult);

    expectedSparkSql = "" + "SELECT COUNT(*) `count_emp`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);

    ppl = "source=EMP | stats count as cnt";
    root = getRelNode(ppl);
    expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], cnt=[COUNT()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "cnt=14\n";
    verifyResult(root, expectedResult);

    expectedSparkSql = "" + "SELECT COUNT(*) `cnt`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCountField() {
    String ppl = "source=EMP | stats count(COMM) as c";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
            + "  LogicalProject(COMM=[$6])\n"
            + "    LogicalFilter(condition=[IS NOT NULL($6)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "c=4\n";
    verifyResult(root, expectedResult);
    String expectedSparkSql =
        "SELECT COUNT(`COMM`) `c`\nFROM `scott`.`EMP`\nWHERE `COMM` IS NOT NULL";
    verifyPPLToSparkSQL(root, expectedSparkSql);

    ppl = "source=EMP | stats count(COMM) as c1, count(COMM) as c2";
    root = getRelNode(ppl);
    expectedLogical =
        ""
            + "LogicalProject(c1=[$0], c2=[$0])\n"
            + "  LogicalAggregate(group=[{}], c1=[COUNT($0)])\n"
            + "    LogicalProject(COMM=[$6])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($6)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "c1=4; c2=4\n";
    verifyResult(root, expectedResult);
    expectedSparkSql =
        "SELECT COUNT(`COMM`) `c1`, COUNT(`COMM`) `c2`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `COMM` IS NOT NULL";
    verifyPPLToSparkSQL(root, expectedSparkSql);

    ppl = "source=EMP | stats count(), count(COMM) as c";
    root = getRelNode(ppl);
    expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], count()=[COUNT()], c=[COUNT($0)])\n"
            + "  LogicalProject(COMM=[$6])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "count()=14; c=4\n";
    verifyResult(root, expectedResult);
    expectedSparkSql = "SELECT COUNT(*) `count()`, COUNT(`COMM`) `c`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);

    ppl = "source=EMP | stats count(COMM + 1.0) as c";
    root = getRelNode(ppl);
    expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
            + "  LogicalProject($f1=[+($6, 1.0:DECIMAL(2, 1))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "c=4\n";
    verifyResult(root, expectedResult);

    expectedSparkSql = "SELECT COUNT(`COMM` + 1.0) `c`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);

    ppl = "source=EMP | stats count(eval(COMM >= 500)) as c";
    root = getRelNode(ppl);
    expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT($0)])\n"
            + "  LogicalProject($f1=[CASE(>=($6, 500), 1, null:NULL)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "c=2\n";
    verifyResult(root, expectedResult);

    expectedSparkSql =
        "SELECT COUNT(CASE WHEN `COMM` >= 500 THEN 1 ELSE NULL END) `c`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);

    ppl = "source=EMP | eval COMM1 = COMM | stats count(COMM) as c, count(COMM1) as c1";
    root = getRelNode(ppl);
    expectedLogical =
        "LogicalAggregate(group=[{}], c=[COUNT($0)], c1=[COUNT($1)])\n"
            + "  LogicalProject(COMM=[$6], COMM1=[$8])\n"
            + "    LogicalFilter(condition=[IS NOT NULL($6)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], COMM1=[$6])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "c=4; c1=4\n";
    verifyResult(root, expectedResult);

    expectedSparkSql =
        "SELECT COUNT(`COMM`) `c`, COUNT(`COMM1`) `c1`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " `COMM` `COMM1`\n"
            + "FROM `scott`.`EMP`) `t12`\n"
            + "WHERE `COMM` IS NOT NULL";
    verifyPPLToSparkSQL(root, expectedSparkSql);

    ppl = "source=EMP | eval COMM1 = COMM + 1 | stats count(COMM) as c, count(COMM1) as c1";
    root = getRelNode(ppl);
    expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT($0)], c1=[COUNT($1)])\n"
            + "  LogicalProject(COMM=[$6], COMM1=[+($6, 1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "c=4; c1=4\n";
    verifyResult(root, expectedResult);

    expectedSparkSql = "SELECT COUNT(`COMM`) `c`, COUNT(`COMM` + 1) `c1`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTakeAgg() {
    String ppl = "source=EMP | stats take(JOB, 2) as c";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], c=[TAKE($0, $1)])\n"
            + "  LogicalProject(JOB=[$2], $f1=[2])\n"
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
            + "LogicalAggregate(group=[{}], avg(SAL)=[AVG($0)])\n"
            + "  LogicalProject(SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg(SAL)=2073.214285\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT AVG(`SAL`) `avg(SAL)`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNonBucketNullableShouldNotImpactAggregateWithoutGroupBy() {
    String ppl = "source=EMP | stats bucket_nullable=false avg(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], avg(SAL)=[AVG($0)])\n"
            + "  LogicalProject(SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg(SAL)=2073.214285\n";
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
        "LogicalAggregate(group=[{}], avg_sal=[AVG($0)], max_sal=[MAX($0)], min_sal=[MIN($0)],"
            + " cnt=[COUNT()])\n"
            + "  LogicalProject(SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg_sal=2073.214285; max_sal=5000.00; min_sal=800.00; cnt=14\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT AVG(`SAL`) `avg_sal`, MAX(`SAL`) `max_sal`, MIN(`SAL`) `min_sal`, COUNT(*) `cnt`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleAggregatesWithCountAbbreviation() {
    String ppl =
        "source=EMP | stats avg(SAL) as avg_sal, max(SAL) as max_sal, min(SAL) as min_sal, c()"
            + " as cnt";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], avg_sal=[AVG($0)], max_sal=[MAX($0)], min_sal=[MIN($0)],"
            + " cnt=[COUNT()])\n"
            + "  LogicalProject(SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg_sal=2073.214285; max_sal=5000.00; min_sal=800.00; cnt=14\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT AVG(`SAL`) `avg_sal`, MAX(`SAL`) `max_sal`, MIN(`SAL`) `min_sal`, COUNT(*) `cnt`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);

    ppl =
        "source=EMP | stats avg(SAL) as avg_sal, max(SAL) as max_sal, min(SAL) as min_sal, count as"
            + " cnt";
    root = getRelNode(ppl);
    expectedLogical =
        "LogicalAggregate(group=[{}], avg_sal=[AVG($0)], max_sal=[MAX($0)], min_sal=[MIN($0)],"
            + " cnt=[COUNT()])\n"
            + "  LogicalProject(SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    expectedResult = "avg_sal=2073.214285; max_sal=5000.00; min_sal=800.00; cnt=14\n";
    verifyResult(root, expectedResult);

    expectedSparkSql =
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
            + "  LogicalAggregate(group=[{0}], avg_sal=[AVG($1)], max_sal=[MAX($1)],"
            + " min_sal=[MIN($1)], cnt=[COUNT()])\n"
            + "    LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg_sal=2175.; max_sal=3000.00; min_sal=800.00; cnt=5; DEPTNO=20\n"
            + "avg_sal=2916.666666; max_sal=5000.00; min_sal=1300.00; cnt=3; DEPTNO=10\n"
            + "avg_sal=1566.666666; max_sal=2850.00; min_sal=950.00; cnt=6; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT AVG(`SAL`) `avg_sal`, MAX(`SAL`) `max_sal`, MIN(`SAL`) `min_sal`,"
            + " COUNT(*) `cnt`, `DEPTNO`\n"
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
            + "LogicalProject(avg(SAL)=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], avg(SAL)=[AVG($1)])\n"
            + "    LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(SAL)=2175.; DEPTNO=20\n"
            + "avg(SAL)=2916.666666; DEPTNO=10\n"
            + "avg(SAL)=1566.666666; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`SAL`) `avg(SAL)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAvgByFieldNonBucketNullable() {
    String ppl = "source=EMP | stats bucket_nullable=false avg(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(avg(SAL)=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], avg(SAL)=[AVG($1)])\n"
            + "    LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(SAL)=2175.; DEPTNO=20\n"
            + "avg(SAL)=2916.666666; DEPTNO=10\n"
            + "avg(SAL)=1566.666666; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT\n"
            + "/*+ `AGG_ARGS`(`ignoreNullBucket` = 'true') */\n"
            + "AVG(`SAL`) `avg(SAL)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IS NOT NULL\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAvgByMultipleFieldsWithDiffOrder() {
    String ppl1 = "source=EMP | stats avg(SAL) by JOB, DEPTNO";
    RelNode root1 = getRelNode(ppl1);
    String expectedLogical1 =
        ""
            + "LogicalProject(avg(SAL)=[$2], JOB=[$0], DEPTNO=[$1])\n"
            + "  LogicalAggregate(group=[{0, 1}], avg(SAL)=[AVG($2)])\n"
            + "    LogicalProject(JOB=[$2], DEPTNO=[$7], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root1, expectedLogical1);

    String expectedSparkSql1 =
        ""
            + "SELECT AVG(`SAL`) `avg(SAL)`, `JOB`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`, `DEPTNO`";
    verifyPPLToSparkSQL(root1, expectedSparkSql1);

    String ppl2 = "source=EMP | stats avg(SAL) by DEPTNO, JOB";
    RelNode root2 = getRelNode(ppl2);
    String expectedLogical2 =
        ""
            + "LogicalProject(avg(SAL)=[$2], DEPTNO=[$0], JOB=[$1])\n"
            + "  LogicalAggregate(group=[{0, 1}], avg(SAL)=[AVG($2)])\n"
            + "    LogicalProject(DEPTNO=[$7], JOB=[$2], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root2, expectedLogical2);

    String expectedSparkSql2 =
        ""
            + "SELECT AVG(`SAL`) `avg(SAL)`, `DEPTNO`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`";
    verifyPPLToSparkSQL(root2, expectedSparkSql2);
  }

  @Test
  public void testAvgBySpan() {
    String ppl = "source=EMP | stats avg(SAL) by span(EMPNO, 100) as empno_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(avg(SAL)=[$1], empno_span=[$0])\n"
            + "  LogicalAggregate(group=[{1}], avg(SAL)=[AVG($0)])\n"
            + "    LogicalProject(SAL=[$5], empno_span=[SPAN($0, 100, null:NULL)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(SAL)=2050.; empno_span=7600\n"
            + "avg(SAL)=800.; empno_span=7300\n"
            + "avg(SAL)=2725.; empno_span=7700\n"
            + "avg(SAL)=1600.; empno_span=7400\n"
            + "avg(SAL)=2533.333333; empno_span=7800\n"
            + "avg(SAL)=2112.5; empno_span=7500\n"
            + "avg(SAL)=1750.; empno_span=7900\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testAvgBySpanAndFields() {
    String ppl =
        "source=EMP | stats avg(SAL) by span(EMPNO, 500) as empno_span, DEPTNO | sort DEPTNO,"
            + " empno_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$2], sort1=[$1], dir0=[ASC-nulls-first], dir1=[ASC-nulls-first])\n"
            + "  LogicalProject(avg(SAL)=[$2], empno_span=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{0, 2}], avg(SAL)=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5], empno_span=[SPAN($0, 500, null:NULL)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "avg(SAL)=2916.666666; empno_span=7500; DEPTNO=10\n"
            + "avg(SAL)=800.; empno_span=7000; DEPTNO=20\n"
            + "avg(SAL)=2518.750000; empno_span=7500; DEPTNO=20\n"
            + "avg(SAL)=1600.; empno_span=7000; DEPTNO=30\n"
            + "avg(SAL)=1560.; empno_span=7500; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`SAL`) `avg(SAL)`, SPAN(`EMPNO`, 500, NULL) `empno_span`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, SPAN(`EMPNO`, 500, NULL)\n"
            + "ORDER BY `DEPTNO`, 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAvgByTimeSpanAndFields() {
    String ppl =
        "source=EMP | stats avg(SAL) by span(HIREDATE, 1year) as hiredate_span, DEPTNO | sort"
            + " DEPTNO, hiredate_span";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$2], sort1=[$1], dir0=[ASC-nulls-first], dir1=[ASC-nulls-first])\n"
            + "  LogicalProject(avg(SAL)=[$2], hiredate_span=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{0, 2}], avg(SAL)=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5], hiredate_span=[SPAN($4, 1, 'y')])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($4)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT AVG(`SAL`) `avg(SAL)`, SPAN(`HIREDATE`, 1, 'y') `hiredate_span`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `HIREDATE` IS NOT NULL\n"
            + "GROUP BY `DEPTNO`, SPAN(`HIREDATE`, 1, 'y')\n"
            + "ORDER BY `DEPTNO`, 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCountDistinct() {
    String ppl = "source=EMP | stats distinct_count(JOB) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(distinct_count(JOB)=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], distinct_count(JOB)=[COUNT(DISTINCT $1)])\n"
            + "    LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "distinct_count(JOB)=3; DEPTNO=20\n"
            + "distinct_count(JOB)=3; DEPTNO=10\n"
            + "distinct_count(JOB)=3; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT COUNT(DISTINCT `JOB`) `distinct_count(JOB)`, `DEPTNO`\n"
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
            + "LogicalProject(dc=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], dc=[COUNT(DISTINCT $1)])\n"
            + "    LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "dc=3; DEPTNO=20\ndc=3; DEPTNO=10\ndc=3; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT COUNT(DISTINCT `JOB`) `dc`, `DEPTNO`\nFROM `scott`.`EMP`\nGROUP BY `DEPTNO`";
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
            + "  LogicalAggregate(group=[{0}], stddev_samp(SAL)=[STDDEV_SAMP($1)])\n"
            + "    LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "stddev_samp(SAL)=1123.332096; DEPTNO=20\n"
            + "stddev_samp(SAL)=1893.629671; DEPTNO=10\n"
            + "stddev_samp(SAL)=668.331255; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT STDDEV_SAMP(`SAL`) `stddev_samp(SAL)`, `DEPTNO`\n"
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
            + "LogicalProject(samp=[$1], empno_span=[$0])\n"
            + "  LogicalAggregate(group=[{1}], samp=[STDDEV_SAMP($0)])\n"
            + "    LogicalProject(SAL=[$5], empno_span=[SPAN($0, 100, null:NULL)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "samp=1131.370849; empno_span=7600\n"
            + "samp=null; empno_span=7300\n"
            + "samp=388.908729; empno_span=7700\n"
            + "samp=null; empno_span=7400\n"
            + "samp=2145.538005; empno_span=7800\n"
            + "samp=1219.759197; empno_span=7500\n"
            + "samp=1096.585609; empno_span=7900\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT STDDEV_SAMP(`SAL`) `samp`, SPAN(`EMPNO`, 100, NULL) `empno_span`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY SPAN(`EMPNO`, 100, NULL)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testStddevPopByField() {
    String ppl = "source=EMP | stats stddev_pop(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(stddev_pop(SAL)=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], stddev_pop(SAL)=[STDDEV_POP($1)])\n"
            + "    LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        ""
            + "stddev_pop(SAL)=1004.738772; DEPTNO=20\n"
            + "stddev_pop(SAL)=1546.142152; DEPTNO=10\n"
            + "stddev_pop(SAL)=610.100173; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT STDDEV_POP(`SAL`) `stddev_pop(SAL)`, `DEPTNO`\n"
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
            + "LogicalProject(pop=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], pop=[STDDEV_POP($1)])\n"
            + "    LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "pop=1004.738772; DEPTNO=20\npop=1546.142152; DEPTNO=10\npop=610.100173; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT STDDEV_POP(`SAL`) `pop`, `DEPTNO`\n"
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
            + "LogicalProject(avg_a=[$1], b=[$0])\n"
            + "  LogicalAggregate(group=[{0}], avg_a=[AVG($1)])\n"
            + "    LogicalProject(b=[1], a=[1])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg_a=1.0; b=1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT AVG(1) `avg_a`, 1 `b`\n" + "FROM `scott`.`EMP`\n" + "GROUP BY 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAggWithBackticksAlias() {
    String ppl = "source=EMP | stats avg(`SAL`) as `avg_sal`";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], avg_sal=[AVG($0)])\n"
            + "  LogicalProject(SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "avg_sal=2073.214285\n";
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
            + "  LogicalAggregate(group=[{}], avg_sal=[AVG($0)])\n"
            + "    LogicalProject(SAL=[$5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "avg_avg_sal=2073.2142850000\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`avg_sal`) `avg_avg_sal`\n"
            + "FROM (SELECT AVG(`SAL`) `avg_sal`\n"
            + "FROM `scott`.`EMP`) `t0`";
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
            + "  LogicalAggregate(group=[{0}], avg_avg_sal=[AVG($1)])\n"
            + "    LogicalProject(MGR=[$1], avg_sal=[$2])\n"
            + "      LogicalAggregate(group=[{0, 1}], avg_sal=[AVG($2)])\n"
            + "        LogicalProject(DEPTNO=[$7], MGR=[$3], SAL=[$5])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "avg_avg_sal=5000.; MGR=null\n"
            + "avg_avg_sal=1310.; MGR=7698\n"
            + "avg_avg_sal=1300.; MGR=7782\n"
            + "avg_avg_sal=1100.; MGR=7788\n"
            + "avg_avg_sal=800.; MGR=7902\n"
            + "avg_avg_sal=3000.; MGR=7566\n"
            + "avg_avg_sal=2758.3333333333; MGR=7839\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT AVG(`avg_sal`) `avg_avg_sal`, `MGR`\n"
            + "FROM (SELECT `MGR`, AVG(`SAL`) `avg_sal`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `MGR`) `t1`\n"
            + "GROUP BY `MGR`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPercentileShortcuts() {
    String ppl = "source=EMP | stats perc50(SAL), p95(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], perc50(SAL)=[percentile_approx($0, $1, $2)],"
            + " p95(SAL)=[percentile_approx($0, $3, $2)])\n"
            + "  LogicalProject(SAL=[$5], $f2=[50.0E0:DOUBLE], $f3=[FLAG(DECIMAL)],"
            + " $f4=[95.0E0:DOUBLE])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `percentile_approx`(`SAL`, 5.00E1, DECIMAL) `perc50(SAL)`,"
            + " `percentile_approx`(`SAL`, 9.50E1, DECIMAL) `p95(SAL)`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPercentileShortcutsWithDecimals() {
    String ppl = "source=EMP | stats perc25.5(SAL), p99.9(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], perc25.5(SAL)=[percentile_approx($0, $1, $2)],"
            + " p99.9(SAL)=[percentile_approx($0, $3, $2)])\n"
            + "  LogicalProject(SAL=[$5], $f2=[25.5E0:DOUBLE], $f3=[FLAG(DECIMAL)],"
            + " $f4=[99.9E0:DOUBLE])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `percentile_approx`(`SAL`, 2.55E1, DECIMAL) `perc25.5(SAL)`,"
            + " `percentile_approx`(`SAL`, 9.99E1, DECIMAL) `p99.9(SAL)`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPercentileShortcutsByField() {
    String ppl = "source=EMP | stats perc75(SAL) by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(perc75(SAL)=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], perc75(SAL)=[percentile_approx($1, $2, $3)])\n"
            + "    LogicalProject(DEPTNO=[$7], SAL=[$5], $f2=[75.0E0:DOUBLE],"
            + " $f3=[FLAG(DECIMAL)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `percentile_approx`(`SAL`, 7.50E1, DECIMAL) `perc75(SAL)`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPercentileShortcutsBoundaryValues() {
    String ppl = "source=EMP | stats perc0(SAL), p100(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], perc0(SAL)=[percentile_approx($0, $1, $2)],"
            + " p100(SAL)=[percentile_approx($0, $3, $2)])\n"
            + "  LogicalProject(SAL=[$5], $f2=[0.0E0:DOUBLE], $f3=[FLAG(DECIMAL)],"
            + " $f4=[100.0E0:DOUBLE])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `percentile_approx`(`SAL`, 0E0, DECIMAL) `perc0(SAL)`,"
            + " `percentile_approx`(`SAL`, 1.000E2, DECIMAL) `p100(SAL)`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test(expected = Exception.class)
  public void testPercentileShortcutInvalidValueAbove100() {
    String ppl = "source=EMP | stats p101(SAL)";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testPercentileShortcutInvalidDecimalValueAbove100() {
    String ppl = "source=EMP | stats perc100.1(SAL)";
    getRelNode(ppl);
  }

  @Test
  public void testMedian() {
    String ppl = "source=EMP | stats median(SAL)";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{}], median(SAL)=[percentile_approx($0, $1, $2)])\n"
            + "  LogicalProject(SAL=[$5], $f1=[50.0:DECIMAL(3, 1)], $f2=[FLAG(DECIMAL)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `percentile_approx`(`SAL`, 50.0, DECIMAL) `median(SAL)`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMaxOnStringField() {
    String ppl = "source=EMP | stats max(ENAME) as max_name";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{}], max_name=[MAX($0)])\n"
            + "  LogicalProject(ENAME=[$1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "max_name=WARD\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "SELECT MAX(`ENAME`) `max_name`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMinOnStringField() {
    String ppl = "source=EMP | stats min(ENAME) as min_name";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{}], min_name=[MIN($0)])\n"
            + "  LogicalProject(ENAME=[$1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "min_name=ADAMS\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "SELECT MIN(`ENAME`) `min_name`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMaxOnTimeField() {
    String ppl = "source=EMP | stats max(HIREDATE) as max_hire_date";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{}], max_hire_date=[MAX($0)])\n"
            + "  LogicalProject(HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "max_hire_date=1987-05-23\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "SELECT MAX(`HIREDATE`) `max_hire_date`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMinOnTimeField() {
    String ppl = "source=EMP | stats min(HIREDATE) as min_hire_date";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{}], min_hire_date=[MIN($0)])\n"
            + "  LogicalProject(HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "min_hire_date=1980-12-17\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "SELECT MIN(`HIREDATE`) `min_hire_date`\nFROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSortAggregationMetrics1() {
    String ppl = "source=EMP | stats bucket_nullable=false avg(SAL) as avg by DEPTNO | sort - avg";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "  LogicalProject(avg=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{0}], avg=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "avg=2916.666666; DEPTNO=10\navg=2175.; DEPTNO=20\navg=1566.666666; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT\n"
            + "/*+ `AGG_ARGS`(`ignoreNullBucket` = 'true') */\n"
            + "AVG(`SAL`) `avg`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IS NOT NULL\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY 1 DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSortAggregationMetrics2() {
    String ppl =
        "source=EMP | stats avg(SAL) as avg by span(HIREDATE, 1year) as hiredate_span | sort"
            + " avg";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalSort(sort0=[$0], dir0=[ASC-nulls-first])\n"
            + "  LogicalProject(avg=[$1], hiredate_span=[$0])\n"
            + "    LogicalAggregate(group=[{1}], avg=[AVG($0)])\n"
            + "      LogicalProject(SAL=[$5], hiredate_span=[SPAN($4, 1, 'y')])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($4)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT\n"
            + "/*+ `AGG_ARGS`(`ignoreNullBucket` = 'true') */\n"
            + "AVG(`SAL`) `avg`, SPAN(`HIREDATE`, 1, 'y') `hiredate_span`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `HIREDATE` IS NOT NULL\n"
            + "GROUP BY SPAN(`HIREDATE`, 1, 'y')\n"
            + "ORDER BY 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testHaving1() {
    String ppl = "source=EMP | stats avg(SAL) as avg by DEPTNO | where avg > 0";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[>($0, 0)])\n"
            + "  LogicalProject(avg=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{0}], avg=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "avg=2175.; DEPTNO=20\navg=2916.666666; DEPTNO=10\navg=1566.666666; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT AVG(`SAL`) `avg`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `t1`\n"
            + "WHERE `avg` > 0";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testHaving2() {
    String ppl =
        "source=EMP | stats bucket_nullable = false avg(SAL) as avg by DEPTNO | where avg > 0";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[>($0, 0)])\n"
            + "  LogicalProject(avg=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{0}], avg=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "avg=2175.; DEPTNO=20\navg=2916.666666; DEPTNO=10\navg=1566.666666; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT\n"
            + "/*+ `AGG_ARGS`(`ignoreNullBucket` = 'true') */\n"
            + "AVG(`SAL`) `avg`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IS NOT NULL\n"
            + "GROUP BY `DEPTNO`) `t2`\n"
            + "WHERE `avg` > 0";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testHaving3() {
    String ppl =
        "source=EMP | stats avg(SAL) as avg, count() as cnt by DEPTNO | eval new_avg = avg + 1000,"
            + " new_cnt = cnt + 1 | where new_avg > 1000 or new_cnt > 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[OR(>($3, 1000), >($4, 2))])\n"
            + "  LogicalProject(avg=[$1], cnt=[$2], DEPTNO=[$0], new_avg=[+($1, 1000)],"
            + " new_cnt=[+($2, 1)])\n"
            + "    LogicalAggregate(group=[{0}], avg=[AVG($1)], cnt=[COUNT()])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "avg=2175.; cnt=5; DEPTNO=20; new_avg=3175.; new_cnt=6\n"
            + "avg=2916.666666; cnt=3; DEPTNO=10; new_avg=3916.666666; new_cnt=4\n"
            + "avg=1566.666666; cnt=6; DEPTNO=30; new_avg=2566.666666; new_cnt=7\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT AVG(`SAL`) `avg`, COUNT(*) `cnt`, `DEPTNO`, AVG(`SAL`) + 1000"
            + " `new_avg`, COUNT(*) + 1 `new_cnt`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`) `t1`\n"
            + "WHERE `new_avg` > 1000 OR `new_cnt` > 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
