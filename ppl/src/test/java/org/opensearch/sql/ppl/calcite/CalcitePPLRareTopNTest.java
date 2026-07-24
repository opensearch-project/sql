/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLRareTopNTest extends CalcitePPLAbstractTest {

  public CalcitePPLRareTopNTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testRare() {
    String ppl = "source=EMP | rare JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$0], count=[$1])\n"
            + "  LogicalFilter(condition=[<=($2, 10)])\n"
            + "    LogicalProject(JOB=[$0], count=[$1], _row_number_rare_top_=[ROW_NUMBER() OVER"
            + " (ORDER BY $1, $0)])\n"
            + "      LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "        LogicalProject(JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "JOB=PRESIDENT; count=1\n"
            + "JOB=ANALYST; count=2\n"
            + "JOB=MANAGER; count=3\n"
            + "JOB=CLERK; count=4\n"
            + "JOB=SALESMAN; count=4\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `JOB`, `count`\n"
            + "FROM (SELECT `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (ORDER BY COUNT(*) NULLS"
            + " LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t1`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareBy() {
    String ppl = "source=EMP | rare JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2, $1)])\n"
            + "      LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=MANAGER; count=1\n"
            + "DEPTNO=20; JOB=ANALYST; count=2\n"
            + "DEPTNO=20; JOB=CLERK; count=2\n"
            + "DEPTNO=10; JOB=CLERK; count=1\n"
            + "DEPTNO=10; JOB=MANAGER; count=1\n"
            + "DEPTNO=10; JOB=PRESIDENT; count=1\n"
            + "DEPTNO=30; JOB=CLERK; count=1\n"
            + "DEPTNO=30; JOB=MANAGER; count=1\n"
            + "DEPTNO=30; JOB=SALESMAN; count=4\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`, `count`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY COUNT(*) NULLS LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t1`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareDisableShowCount() {
    String ppl = "source=EMP | rare showcount=false JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2, $1)])\n"
            + "      LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=MANAGER\n"
            + "DEPTNO=20; JOB=ANALYST\n"
            + "DEPTNO=20; JOB=CLERK\n"
            + "DEPTNO=10; JOB=CLERK\n"
            + "DEPTNO=10; JOB=MANAGER\n"
            + "DEPTNO=10; JOB=PRESIDENT\n"
            + "DEPTNO=30; JOB=CLERK\n"
            + "DEPTNO=30; JOB=MANAGER\n"
            + "DEPTNO=30; JOB=SALESMAN\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY COUNT(*) NULLS LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t1`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareCountField() {
    String ppl = "source=EMP | rare countfield='my_cnt' JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1], my_cnt=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], my_cnt=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2, $1)])\n"
            + "      LogicalAggregate(group=[{0, 1}], my_cnt=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=MANAGER; my_cnt=1\n"
            + "DEPTNO=20; JOB=ANALYST; my_cnt=2\n"
            + "DEPTNO=20; JOB=CLERK; my_cnt=2\n"
            + "DEPTNO=10; JOB=CLERK; my_cnt=1\n"
            + "DEPTNO=10; JOB=MANAGER; my_cnt=1\n"
            + "DEPTNO=10; JOB=PRESIDENT; my_cnt=1\n"
            + "DEPTNO=30; JOB=CLERK; my_cnt=1\n"
            + "DEPTNO=30; JOB=MANAGER; my_cnt=1\n"
            + "DEPTNO=30; JOB=SALESMAN; my_cnt=4\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`, `my_cnt`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `my_cnt`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY COUNT(*) NULLS LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t1`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareUseNullFalse() {
    String ppl = "source=EMP | rare usenull=false JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2, $1)])\n"
            + "      LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "          LogicalFilter(condition=[AND(IS NOT NULL($7), IS NOT NULL($2))])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=MANAGER; count=1\n"
            + "DEPTNO=20; JOB=ANALYST; count=2\n"
            + "DEPTNO=20; JOB=CLERK; count=2\n"
            + "DEPTNO=10; JOB=CLERK; count=1\n"
            + "DEPTNO=10; JOB=MANAGER; count=1\n"
            + "DEPTNO=10; JOB=PRESIDENT; count=1\n"
            + "DEPTNO=30; JOB=CLERK; count=1\n"
            + "DEPTNO=30; JOB=MANAGER; count=1\n"
            + "DEPTNO=30; JOB=SALESMAN; count=4\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`, `count`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY COUNT(*) NULLS LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IS NOT NULL AND `JOB` IS NOT NULL\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void failWithDuplicatedName() {
    try {
      RelNode root = getRelNode("source=EMP | eval count=1 | rare JOB by count");
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(
          e.getMessage(),
          is("Field `count` is existed, change the count field by setting countfield='xyz'"));
    }
    try {
      RelNode root = getRelNode("source=EMP | rare countfield='DEPTNO' JOB by DEPTNO");
      fail("expected error, got " + root);
    } catch (Exception e) {
      assertThat(
          e.getMessage(),
          is("Field `DEPTNO` is existed, change the count field by setting countfield='xyz'"));
    }
  }

  @Test
  public void testRareShowPerc() {
    String ppl = "source=EMP | rare showperc=true JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(JOB=[$0], count=[$1], percent=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(JOB=[$0], count=[$1], percent=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (ORDER BY $1, $0)])\n"
            + "      LogicalProject(JOB=[$0], count=[$1],"
            + " percent=[ROUND(/(*(100.0:DECIMAL(4, 1), CAST($1):DOUBLE NOT NULL),"
            + " CAST(SUM($1) OVER ()):DOUBLE NOT NULL), 2)])\n"
            + "        LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "          LogicalProject(JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "JOB=PRESIDENT; count=1; percent=7.14\n"
            + "JOB=ANALYST; count=2; percent=14.29\n"
            + "JOB=MANAGER; count=3; percent=21.43\n"
            + "JOB=CLERK; count=4; percent=28.57\n"
            + "JOB=SALESMAN; count=4; percent=28.57\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `JOB`, `count`, `percent`\n"
            + "FROM (SELECT `JOB`, `count`, `percent`, ROW_NUMBER() OVER (ORDER BY `count` NULLS"
            + " LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM (SELECT `JOB`, COUNT(*) `count`, ROUND(100.0 * CAST(COUNT(*) AS DOUBLE) /"
            + " CAST(SUM(COUNT(*)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE), 2) `percent`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t1`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareShowPercWithGroupBy() {
    String ppl = "source=EMP | rare showperc=true JOB by DEPTNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2], percent=[$3])\n"
            + "  LogicalFilter(condition=[<=($4, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2], percent=[$3],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2, $1)])\n"
            + "      LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2],"
            + " percent=[ROUND(/(*(100.0:DECIMAL(4, 1), CAST($2):DOUBLE NOT NULL),"
            + " CAST(SUM($2) OVER (PARTITION BY $0)):DOUBLE NOT NULL), 2)])\n"
            + "        LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "          LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=MANAGER; count=1; percent=20.0\n"
            + "DEPTNO=20; JOB=ANALYST; count=2; percent=40.0\n"
            + "DEPTNO=20; JOB=CLERK; count=2; percent=40.0\n"
            + "DEPTNO=10; JOB=CLERK; count=1; percent=33.33\n"
            + "DEPTNO=10; JOB=MANAGER; count=1; percent=33.33\n"
            + "DEPTNO=10; JOB=PRESIDENT; count=1; percent=33.33\n"
            + "DEPTNO=30; JOB=CLERK; count=1; percent=16.67\n"
            + "DEPTNO=30; JOB=MANAGER; count=1; percent=16.67\n"
            + "DEPTNO=30; JOB=SALESMAN; count=4; percent=66.67\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`, `count`, `percent`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, `count`, `percent`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY `count` NULLS LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROUND(100.0 * CAST(COUNT(*) AS"
            + " DOUBLE) / CAST(SUM(COUNT(*)) OVER (PARTITION BY `DEPTNO` RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING) AS DOUBLE), 2) `percent`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t1`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareShowPercWithoutShowCount() {
    String ppl = "source=EMP | rare showcount=false showperc=true JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(JOB=[$0], percent=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(JOB=[$0], count=[$1], percent=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (ORDER BY $1, $0)])\n"
            + "      LogicalProject(JOB=[$0], count=[$1],"
            + " percent=[ROUND(/(*(100.0:DECIMAL(4, 1), CAST($1):DOUBLE NOT NULL),"
            + " CAST(SUM($1) OVER ()):DOUBLE NOT NULL), 2)])\n"
            + "        LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "          LogicalProject(JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "JOB=PRESIDENT; percent=7.14\n"
            + "JOB=ANALYST; percent=14.29\n"
            + "JOB=MANAGER; percent=21.43\n"
            + "JOB=CLERK; percent=28.57\n"
            + "JOB=SALESMAN; percent=28.57\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `JOB`, `percent`\n"
            + "FROM (SELECT `JOB`, `count`, `percent`, ROW_NUMBER() OVER (ORDER BY `count` NULLS"
            + " LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM (SELECT `JOB`, COUNT(*) `count`, ROUND(100.0 * CAST(COUNT(*) AS DOUBLE) /"
            + " CAST(SUM(COUNT(*)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE), 2) `percent`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t1`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRareShowPercWithLimit() {
    String ppl = "source=EMP | rare 1 showperc=true JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(JOB=[$0], count=[$1], percent=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 1)])\n"
            + "    LogicalProject(JOB=[$0], count=[$1], percent=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (ORDER BY $1, $0)])\n"
            + "      LogicalProject(JOB=[$0], count=[$1],"
            + " percent=[ROUND(/(*(100.0:DECIMAL(4, 1), CAST($1):DOUBLE NOT NULL),"
            + " CAST(SUM($1) OVER ()):DOUBLE NOT NULL), 2)])\n"
            + "        LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "          LogicalProject(JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    // Should show percentage relative to full dataset, not 100%
    // PRESIDENT has 1 out of 14 total employees = 7.14%
    String expectedResult = "JOB=PRESIDENT; count=1; percent=7.14\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `JOB`, `count`, `percent`\n"
            + "FROM (SELECT `JOB`, `count`, `percent`, ROW_NUMBER() OVER (ORDER BY `count` NULLS"
            + " LAST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM (SELECT `JOB`, COUNT(*) `count`, ROUND(100.0 * CAST(COUNT(*) AS DOUBLE) /"
            + " CAST(SUM(COUNT(*)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE), 2) `percent`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t1`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTop() {
    String ppl = "source=EMP | top JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$0], count=[$1])\n"
            + "  LogicalFilter(condition=[<=($2, 10)])\n"
            + "    LogicalProject(JOB=[$0], count=[$1], _row_number_rare_top_=[ROW_NUMBER() OVER"
            + " (ORDER BY $1 DESC, $0)])\n"
            + "      LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "        LogicalProject(JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "JOB=CLERK; count=4\n"
            + "JOB=SALESMAN; count=4\n"
            + "JOB=MANAGER; count=3\n"
            + "JOB=ANALYST; count=2\n"
            + "JOB=PRESIDENT; count=1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `JOB`, `count`\n"
            + "FROM (SELECT `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC"
            + " NULLS FIRST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t1`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopBy() {
    String ppl = "source=EMP | top JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2 DESC, $1)])\n"
            + "      LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=ANALYST; count=2\n"
            + "DEPTNO=20; JOB=CLERK; count=2\n"
            + "DEPTNO=20; JOB=MANAGER; count=1\n"
            + "DEPTNO=10; JOB=CLERK; count=1\n"
            + "DEPTNO=10; JOB=MANAGER; count=1\n"
            + "DEPTNO=10; JOB=PRESIDENT; count=1\n"
            + "DEPTNO=30; JOB=SALESMAN; count=4\n"
            + "DEPTNO=30; JOB=CLERK; count=1\n"
            + "DEPTNO=30; JOB=MANAGER; count=1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`, `count`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY COUNT(*) DESC NULLS FIRST, `JOB` NULLS LAST)"
            + " `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t1`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopDisableShowCount() {
    String ppl = "source=EMP | top showcount=false JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2 DESC, $1)])\n"
            + "      LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=ANALYST\n"
            + "DEPTNO=20; JOB=CLERK\n"
            + "DEPTNO=20; JOB=MANAGER\n"
            + "DEPTNO=10; JOB=CLERK\n"
            + "DEPTNO=10; JOB=MANAGER\n"
            + "DEPTNO=10; JOB=PRESIDENT\n"
            + "DEPTNO=30; JOB=SALESMAN\n"
            + "DEPTNO=30; JOB=CLERK\n"
            + "DEPTNO=30; JOB=MANAGER\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY COUNT(*) DESC NULLS FIRST, `JOB` NULLS LAST)"
            + " `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t1`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopCountField() {
    String ppl = "source=EMP | top countfield='my_cnt' JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1], my_cnt=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], my_cnt=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2 DESC, $1)])\n"
            + "      LogicalAggregate(group=[{0, 1}], my_cnt=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=ANALYST; my_cnt=2\n"
            + "DEPTNO=20; JOB=CLERK; my_cnt=2\n"
            + "DEPTNO=20; JOB=MANAGER; my_cnt=1\n"
            + "DEPTNO=10; JOB=CLERK; my_cnt=1\n"
            + "DEPTNO=10; JOB=MANAGER; my_cnt=1\n"
            + "DEPTNO=10; JOB=PRESIDENT; my_cnt=1\n"
            + "DEPTNO=30; JOB=SALESMAN; my_cnt=4\n"
            + "DEPTNO=30; JOB=CLERK; my_cnt=1\n"
            + "DEPTNO=30; JOB=MANAGER; my_cnt=1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`, `my_cnt`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `my_cnt`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY COUNT(*) DESC NULLS FIRST, `JOB` NULLS LAST)"
            + " `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t1`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopUseNullFalse() {
    String ppl = "source=EMP | top usenull=false JOB by DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2 DESC, $1)])\n"
            + "      LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "          LogicalFilter(condition=[AND(IS NOT NULL($7), IS NOT NULL($2))])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=ANALYST; count=2\n"
            + "DEPTNO=20; JOB=CLERK; count=2\n"
            + "DEPTNO=20; JOB=MANAGER; count=1\n"
            + "DEPTNO=10; JOB=CLERK; count=1\n"
            + "DEPTNO=10; JOB=MANAGER; count=1\n"
            + "DEPTNO=10; JOB=PRESIDENT; count=1\n"
            + "DEPTNO=30; JOB=SALESMAN; count=4\n"
            + "DEPTNO=30; JOB=CLERK; count=1\n"
            + "DEPTNO=30; JOB=MANAGER; count=1\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`, `count`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY COUNT(*) DESC NULLS FIRST, `JOB` NULLS LAST)"
            + " `_row_number_rare_top_`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IS NOT NULL AND `JOB` IS NOT NULL\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopShowPerc() {
    String ppl = "source=EMP | top showperc=true JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(JOB=[$0], count=[$1], percent=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(JOB=[$0], count=[$1], percent=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (ORDER BY $1 DESC, $0)])\n"
            + "      LogicalProject(JOB=[$0], count=[$1],"
            + " percent=[ROUND(/(*(100.0:DECIMAL(4, 1), CAST($1):DOUBLE NOT NULL),"
            + " CAST(SUM($1) OVER ()):DOUBLE NOT NULL), 2)])\n"
            + "        LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "          LogicalProject(JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "JOB=CLERK; count=4; percent=28.57\n"
            + "JOB=SALESMAN; count=4; percent=28.57\n"
            + "JOB=MANAGER; count=3; percent=21.43\n"
            + "JOB=ANALYST; count=2; percent=14.29\n"
            + "JOB=PRESIDENT; count=1; percent=7.14\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `JOB`, `count`, `percent`\n"
            + "FROM (SELECT `JOB`, `count`, `percent`, ROW_NUMBER() OVER (ORDER BY `count` DESC"
            + " NULLS FIRST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM (SELECT `JOB`, COUNT(*) `count`, ROUND(100.0 * CAST(COUNT(*) AS DOUBLE) /"
            + " CAST(SUM(COUNT(*)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE), 2) `percent`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t1`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopShowPercWithGroupBy() {
    String ppl = "source=EMP | top showperc=true JOB by DEPTNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2], percent=[$3])\n"
            + "  LogicalFilter(condition=[<=($4, 10)])\n"
            + "    LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2], percent=[$3],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $2 DESC, $1)])\n"
            + "      LogicalProject(DEPTNO=[$0], JOB=[$1], count=[$2],"
            + " percent=[ROUND(/(*(100.0:DECIMAL(4, 1), CAST($2):DOUBLE NOT NULL),"
            + " CAST(SUM($2) OVER (PARTITION BY $0)):DOUBLE NOT NULL), 2)])\n"
            + "        LogicalAggregate(group=[{0, 1}], count=[COUNT()])\n"
            + "          LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "DEPTNO=20; JOB=ANALYST; count=2; percent=40.0\n"
            + "DEPTNO=20; JOB=CLERK; count=2; percent=40.0\n"
            + "DEPTNO=20; JOB=MANAGER; count=1; percent=20.0\n"
            + "DEPTNO=10; JOB=CLERK; count=1; percent=33.33\n"
            + "DEPTNO=10; JOB=MANAGER; count=1; percent=33.33\n"
            + "DEPTNO=10; JOB=PRESIDENT; count=1; percent=33.33\n"
            + "DEPTNO=30; JOB=SALESMAN; count=4; percent=66.67\n"
            + "DEPTNO=30; JOB=CLERK; count=1; percent=16.67\n"
            + "DEPTNO=30; JOB=MANAGER; count=1; percent=16.67\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `JOB`, `count`, `percent`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, `count`, `percent`, ROW_NUMBER() OVER (PARTITION BY"
            + " `DEPTNO` ORDER BY `count` DESC NULLS FIRST, `JOB` NULLS LAST)"
            + " `_row_number_rare_top_`\n"
            + "FROM (SELECT `DEPTNO`, `JOB`, COUNT(*) `count`, ROUND(100.0 * CAST(COUNT(*) AS"
            + " DOUBLE) / CAST(SUM(COUNT(*)) OVER (PARTITION BY `DEPTNO` RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING) AS DOUBLE), 2) `percent`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`, `JOB`) `t1`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopShowPercWithoutShowCount() {
    String ppl = "source=EMP | top showcount=false showperc=true JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(JOB=[$0], percent=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 10)])\n"
            + "    LogicalProject(JOB=[$0], count=[$1], percent=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (ORDER BY $1 DESC, $0)])\n"
            + "      LogicalProject(JOB=[$0], count=[$1],"
            + " percent=[ROUND(/(*(100.0:DECIMAL(4, 1), CAST($1):DOUBLE NOT NULL),"
            + " CAST(SUM($1) OVER ()):DOUBLE NOT NULL), 2)])\n"
            + "        LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "          LogicalProject(JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        ""
            + "JOB=CLERK; percent=28.57\n"
            + "JOB=SALESMAN; percent=28.57\n"
            + "JOB=MANAGER; percent=21.43\n"
            + "JOB=ANALYST; percent=14.29\n"
            + "JOB=PRESIDENT; percent=7.14\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `JOB`, `percent`\n"
            + "FROM (SELECT `JOB`, `count`, `percent`, ROW_NUMBER() OVER (ORDER BY `count` DESC"
            + " NULLS FIRST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM (SELECT `JOB`, COUNT(*) `count`, ROUND(100.0 * CAST(COUNT(*) AS DOUBLE) /"
            + " CAST(SUM(COUNT(*)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE), 2) `percent`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t1`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTopShowPercWithLimit() {
    String ppl = "source=EMP | top 1 showperc=true JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(JOB=[$0], count=[$1], percent=[$2])\n"
            + "  LogicalFilter(condition=[<=($3, 1)])\n"
            + "    LogicalProject(JOB=[$0], count=[$1], percent=[$2],"
            + " _row_number_rare_top_=[ROW_NUMBER() OVER (ORDER BY $1 DESC, $0)])\n"
            + "      LogicalProject(JOB=[$0], count=[$1],"
            + " percent=[ROUND(/(*(100.0:DECIMAL(4, 1), CAST($1):DOUBLE NOT NULL),"
            + " CAST(SUM($1) OVER ()):DOUBLE NOT NULL), 2)])\n"
            + "        LogicalAggregate(group=[{0}], count=[COUNT()])\n"
            + "          LogicalProject(JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    // Should show percentage relative to full dataset, not 100%
    // CLERK has 4 out of 14 total employees = 28.57%
    String expectedResult = "JOB=CLERK; count=4; percent=28.57\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `JOB`, `count`, `percent`\n"
            + "FROM (SELECT `JOB`, `count`, `percent`, ROW_NUMBER() OVER (ORDER BY `count` DESC"
            + " NULLS FIRST, `JOB` NULLS LAST) `_row_number_rare_top_`\n"
            + "FROM (SELECT `JOB`, COUNT(*) `count`, ROUND(100.0 * CAST(COUNT(*) AS DOUBLE) /"
            + " CAST(SUM(COUNT(*)) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE), 2) `percent`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t1`) `t2`\n"
            + "WHERE `_row_number_rare_top_` <= 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
