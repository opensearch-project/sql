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
    // Optimization rule may show double sorts in logical plan but physical execution is optimized
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "  LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `ENAME`) `t`\n"
            + "ORDER BY `ENAME` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDoubleReverseParserSuccess() {
    String ppl = "source=EMP | reverse | reverse";
    RelNode root = getRelNode(ppl);
    // Without optimization rule, shows consecutive sorts
    String expectedLogical =
        "LogicalSort(sort0=[$0], dir0=[ASC])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `EMPNO` DESC NULLS FIRST) `t`\n"
            + "ORDER BY `EMPNO` NULLS LAST";
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
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "  LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "    LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `SAL`) `t`\n"
            + "ORDER BY `ENAME` DESC) `t0`\n"
            + "ORDER BY `ENAME`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultiFieldSortWithReverseParserSuccess() {
    String ppl = "source=EMP | sort + SAL, - ENAME | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$5], sort1=[$1], dir0=[DESC-nulls-last], dir1=[ASC-nulls-first])\n"
            + "  LogicalSort(sort0=[$5], sort1=[$1], dir0=[ASC-nulls-first],"
            + " dir1=[DESC-nulls-last])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `SAL`, `ENAME` DESC) `t`\n"
            + "ORDER BY `SAL` DESC, `ENAME`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testComplexMultiFieldSortWithReverseParserSuccess() {
    String ppl = "source=EMP | sort DEPTNO, + SAL, - ENAME | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$7], sort1=[$5], sort2=[$1], dir0=[DESC-nulls-last],"
            + " dir1=[DESC-nulls-last], dir2=[ASC-nulls-first])\n"
            + "  LogicalSort(sort0=[$7], sort1=[$5], sort2=[$1], dir0=[ASC-nulls-first],"
            + " dir1=[ASC-nulls-first], dir2=[DESC-nulls-last])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `DEPTNO`, `SAL`, `ENAME` DESC) `t`\n"
            + "ORDER BY `DEPTNO` DESC, `SAL` DESC, `ENAME`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReverseWithFieldsAndSortParserSuccess() {
    String ppl = "source=EMP | fields ENAME, SAL, DEPTNO | sort + SAL | reverse";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "  LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])\n"
            + "    LogicalProject(ENAME=[$1], SAL=[$5], DEPTNO=[$7])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `ENAME`, `SAL`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "ORDER BY `SAL`) `t0`\n"
            + "ORDER BY `SAL` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testHeadThenSortReverseNoOpt() {
    // Tests fetch limit behavior: head 5 | sort field | reverse
    // Should NOT be optimized to preserve "take first 5, then sort" semantics
    String ppl = "source=EMP | head 5 | sort + SAL | reverse";
    RelNode root = getRelNode(ppl);

    // Should have three LogicalSort nodes: fetch=5, sort SAL, reverse
    // Calcite's built-in optimization will handle the physical plan optimization
    String expectedLogical =
        "LogicalSort(sort0=[$5], dir0=[DESC-nulls-last])\n"
            + "  LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "    LogicalSort(fetch=[5])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 5) `t`\n"
            + "ORDER BY `SAL`) `t0`\n"
            + "ORDER BY `SAL` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
