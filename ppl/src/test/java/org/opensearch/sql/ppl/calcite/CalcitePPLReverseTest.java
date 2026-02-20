/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Tests for reverse command optimization.
 *
 * <p>The reverse command behavior depends on the presence of: 1. Existing collation (sort): Reverse
 * the sort direction 2. @timestamp field: Sort by @timestamp DESC 3. Neither: No-op (ignore reverse
 * command)
 *
 * <p>These tests use SCOTT_WITH_TEMPORAL schema where EMP table has a default collation on EMPNO
 * (primary key), demonstrating case #1 (reverse existing collation).
 *
 * <p>For @timestamp and no-op cases, see CalciteReverseCommandIT integration tests.
 */
public class CalcitePPLReverseTest extends CalcitePPLAbstractTest {
  public CalcitePPLReverseTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testReverseParserSuccess() {
    // EMP table has default collation on EMPNO, so reverse flips it to DESC
    String ppl = "source=EMP | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$0], dir0=[DESC])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00;"
            + " COMM=null; DEPTNO=20\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT *\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseWithSortParserSuccess() {
    String ppl = "source=EMP | sort ENAME | reverse";
    RelNode root = getRelNode(ppl);
    // Reverse replaces the existing sort in-place, producing a single sort with reversed direction
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT *\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `ENAME` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDoubleReverseParserSuccess() {
    String ppl = "source=EMP | reverse | reverse";
    RelNode root = getRelNode(ppl);
    // Double reverse: first reverse flips ASC->DESC, second reverse flips DESC->ASC
    // Result is back to original order with a single sort node
    String expectedLogical =
        "LogicalSort(sort0=[$0], dir0=[ASC])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT *\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `EMPNO` NULLS LAST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseWithHeadParserSuccess() {
    String ppl = "source=EMP | reverse | head 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$0], dir0=[DESC], fetch=[2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT *\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `EMPNO` DESC NULLS FIRST\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test(expected = Exception.class)
  public void testReverseWithNumberShouldFail() {
    String ppl = "source=EMP | reverse 2";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReverseWithFieldShouldFail() {
    String ppl = "source=EMP | reverse EMPNO";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReverseWithStringShouldFail() {
    String ppl = "source=EMP | reverse \"desc\"";
    getRelNode(ppl);
  }

  @Test(expected = Exception.class)
  public void testReverseWithExpressionShouldFail() {
    String ppl = "source=EMP | reverse EMPNO + 1";
    getRelNode(ppl);
  }

  @Test
  public void testMultipleSortsWithReverseParserSuccess() {
    String ppl = "source=EMP | sort + SAL | sort - ENAME | reverse";
    RelNode root = getRelNode(ppl);
    // Reverse replaces the last sort (- ENAME DESC) in-place, flipping to ASC
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `SAL`) `t`\n"
            + "ORDER BY `ENAME`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultiFieldSortWithReverseParserSuccess() {
    String ppl = "source=EMP | sort + SAL, - ENAME | reverse";
    RelNode root = getRelNode(ppl);
    // Reverse replaces the multi-field sort in-place, flipping each field's direction
    String expectedLogical =
        "LogicalSort(sort0=[$5], sort1=[$1], dir0=[DESC-nulls-last], dir1=[ASC-nulls-first])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `SAL` DESC, `ENAME`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testComplexMultiFieldSortWithReverseParserSuccess() {
    String ppl = "source=EMP | sort DEPTNO, + SAL, - ENAME | reverse";
    RelNode root = getRelNode(ppl);
    // Reverse replaces the 3-field sort in-place, flipping each direction
    String expectedLogical =
        "LogicalSort(sort0=[$7], sort1=[$5], sort2=[$1], dir0=[DESC-nulls-last],"
            + " dir1=[DESC-nulls-last], dir2=[ASC-nulls-first])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `DEPTNO` DESC, `SAL` DESC, `ENAME`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseWithFieldsAndSortParserSuccess() {
    String ppl = "source=EMP | fields ENAME, SAL, DEPTNO | sort + SAL | reverse";
    RelNode root = getRelNode(ppl);
    // Reverse replaces the sort on SAL in-place
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "  LogicalProject(ENAME=[$1], SAL=[$5], DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `SAL`, `DEPTNO`\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `SAL` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSortHeadReverse() {
    // Tests "sort | head | reverse": reverse must be applied after the limit,
    // not merged into the sort+fetch node, to preserve correct semantics.
    String ppl = "source=EMP | sort SAL | head 5 | reverse";
    RelNode root = getRelNode(ppl);

    // The reversed sort sits above the limit+sort node
    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[DESC-nulls-last])\n"
            + "  LogicalSort(sort0=[$5], dir0=[ASC-nulls-first], fetch=[5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testHeadThenSortReverseNoOpt() {
    // Tests fetch limit behavior: head 5 | sort field | reverse
    // Reverse replaces the sort on SAL in-place, preserving the head limit below
    String ppl = "source=EMP | head 5 | sort + SAL | reverse";
    RelNode root = getRelNode(ppl);

    // Two LogicalSort nodes: reversed sort on SAL, then fetch=5
    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[DESC-nulls-last])\n"
            + "  LogicalSort(fetch=[5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 5) `t`\n"
            + "ORDER BY `SAL` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSortFieldsReverse() {
    // Test backtracking: sort on SAL, then project only ENAME, then reverse
    // The sort field (SAL) is removed from schema by fields command
    // But reverse should still work by backtracking to find the sort and replacing it in-place
    String ppl = "source=EMP | sort SAL | fields ENAME | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$5], dir0=[DESC-nulls-last])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `SAL` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  // ==================== Complex query tests with blocking operators ====================
  // These tests verify that reverse becomes a no-op after blocking operators
  // that destroy collation (aggregate, join, set ops, window functions).
  // Since SCOTT_WITH_TEMPORAL schema has no @timestamp field, reverse is ignored.

  @Test
  public void testReverseAfterAggregationIsNoOp() {
    // Aggregation destroys input ordering, so reverse has no collation to reverse
    // and no @timestamp field exists, so reverse should be a no-op
    String ppl = "source=EMP | stats count() as c by DEPTNO | reverse";
    RelNode root = getRelNode(ppl);
    // No additional sort node for reverse - it's a no-op after aggregation
    // Note: There's a project for column reordering (c, DEPTNO) in the output
    String expectedLogical =
        "LogicalProject(c=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], c=[COUNT()])\n"
            + "    LogicalProject(DEPTNO=[$7])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `c`, `DEPTNO`\n" + "FROM `scott`.`EMP`\n" + "GROUP BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseAfterJoinIsNoOp() {
    // Join destroys input ordering, so reverse has no collation to reverse
    // and no @timestamp field exists, so reverse should be a no-op
    String ppl = "source=EMP | join on EMP.DEPTNO = DEPT.DEPTNO DEPT | reverse";
    RelNode root = getRelNode(ppl);
    // No additional sort node for reverse - it's a no-op after join
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], DEPT.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `DEPT.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseAfterSortAndAggregationIsNoOp() {
    // Even if there's a sort before aggregation, aggregation destroys the collation
    // so reverse after aggregation should be a no-op
    String ppl = "source=EMP | sort SAL | stats count() as c by DEPTNO | reverse";
    RelNode root = getRelNode(ppl);
    // Sort before aggregation is present, but reverse after aggregation is a no-op
    // Note: There's a project for column reordering (c, DEPTNO) in the output
    String expectedLogical =
        "LogicalProject(c=[$1], DEPTNO=[$0])\n"
            + "  LogicalAggregate(group=[{0}], c=[COUNT()])\n"
            + "    LogicalProject(DEPTNO=[$7])\n"
            + "      LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    // Verify result data - reverse is a no-op, so data remains in aggregation order
    String expectedResult = "c=5; DEPTNO=20\n" + "c=3; DEPTNO=10\n" + "c=6; DEPTNO=30\n";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testReverseAfterWhereWithSort() {
    // Filter (where) doesn't destroy collation, so reverse should work through it
    String ppl = "source=EMP | sort SAL | where DEPTNO = 10 | reverse";
    RelNode root = getRelNode(ppl);
    // Reverse backtracks through filter to find the sort and inserts reversed sort
    // after the original sort, then the filter is applied on top
    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[DESC-nulls-last])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `SAL`) `t`\n"
            + "WHERE `DEPTNO` = 10\n"
            + "ORDER BY `SAL` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseAfterEvalWithSort() {
    // Eval (project) doesn't destroy collation, so reverse should work through it
    String ppl = "source=EMP | sort SAL | eval bonus = SAL * 0.1 | reverse";
    RelNode root = getRelNode(ppl);
    // Reversed sort is added on top of the project (eval)
    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[DESC-nulls-last])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], bonus=[*($5, 0.1:DECIMAL(2, 1))])\n"
            + "    LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testReverseAfterMultipleFiltersWithSort() {
    // Multiple filters don't destroy collation (Calcite merges consecutive filters)
    String ppl = "source=EMP | sort SAL | where DEPTNO = 10 | where SAL > 1000 | reverse";
    RelNode root = getRelNode(ppl);
    // Reversed sort is added on top of the merged filter
    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[DESC-nulls-last])\n"
            + "  LogicalFilter(condition=[AND(=($7, 10), >($5, 1000))])\n"
            + "    LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testReverseSortJoinSort() {
    // Sort before join, then another sort after join, reverse should work
    String ppl =
        "source=EMP | sort SAL | join on EMP.DEPTNO = DEPT.DEPTNO DEPT | sort DNAME | reverse";
    RelNode root = getRelNode(ppl);
    // The sort before join is destroyed by join, but sort after join can be reversed
    // Reverse replaces the sort on DNAME in-place
    String expectedLogical =
        "LogicalSort(sort0=[$9], dir0=[DESC-nulls-last])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPT.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "    LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "      LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testReverseAfterAggregationWithSort() {
    // Sort after aggregation, then reverse should work
    // Reverse replaces the sort on DEPTNO in-place
    String ppl = "source=EMP | stats count() as c by DEPTNO | sort DEPTNO | reverse";
    RelNode root = getRelNode(ppl);
    // Note: There's a project for column reordering (c, DEPTNO) so DEPTNO is at position 1
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "  LogicalProject(c=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{0}], c=[COUNT()])\n"
            + "      LogicalProject(DEPTNO=[$7])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT COUNT(*) `c`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY `DEPTNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
