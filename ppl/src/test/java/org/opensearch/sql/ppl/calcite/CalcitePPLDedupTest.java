/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLDedupTest extends CalcitePPLAbstractTest {

  public CalcitePPLDedupTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testDedup1() {
    String ppl = "source=EMP | dedup 1 DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[<=($8, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION"
            + " BY $7)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER (PARTITION BY `DEPTNO`) `_row_number_dedup_`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IS NOT NULL) `t0`\n"
            + "WHERE `_row_number_dedup_` <= 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDedup2() {
    String ppl = "source=EMP | dedup 2 DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[<=($8, 2)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION"
            + " BY $7)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER (PARTITION BY `DEPTNO`) `_row_number_dedup_`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IS NOT NULL) `t0`\n"
            + "WHERE `_row_number_dedup_` <= 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDedupKeepEmpty1() {
    String ppl = "source=EMP | dedup 1 DEPTNO, JOB keepempty=true";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[OR(IS NULL($7), IS NULL($2), <=($8, 1))])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION"
            + " BY $7, $2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER (PARTITION BY `DEPTNO`, `JOB`) `_row_number_dedup_`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "WHERE `DEPTNO` IS NULL OR `JOB` IS NULL OR `_row_number_dedup_` <= 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDedupKeepEmpty2() {
    String ppl = "source=EMP | dedup 2 DEPTNO, JOB keepempty=true";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[OR(IS NULL($7), IS NULL($2), <=($8, 2))])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION"
            + " BY $7, $2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER (PARTITION BY `DEPTNO`, `JOB`) `_row_number_dedup_`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "WHERE `DEPTNO` IS NULL OR `JOB` IS NULL OR `_row_number_dedup_` <= 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDedupExpr() {
    String ppl =
        "source=EMP | eval NEW_DEPTNO = DEPTNO + 1 | fields EMPNO, ENAME, JOB, DEPTNO, NEW_DEPTNO |"
            + " dedup 1 NEW_DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], DEPTNO=[$3], NEW_DEPTNO=[$4])\n"
            + "  LogicalFilter(condition=[<=($5, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], DEPTNO=[$3], NEW_DEPTNO=[$4],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $4)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($4)])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], DEPTNO=[$7],"
            + " NEW_DEPTNO=[+($7, 1)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    ppl =
        "source=EMP | fields EMPNO, ENAME, JOB, DEPTNO | eval NEW_DEPTNO = DEPTNO + 1 | dedup 1"
            + " NEW_DEPTNO";
    root = getRelNode(ppl);
    verifyLogical(root, expectedLogical);
    ppl =
        "source=EMP | eval NEW_DEPTNO = DEPTNO + 1 | fields NEW_DEPTNO, EMPNO, ENAME, JOB | dedup 1"
            + " JOB";
    root = getRelNode(ppl);
    expectedLogical =
        "LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3])\n"
            + "  LogicalFilter(condition=[<=($4, 1)])\n"
            + "    LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $3)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($3)])\n"
            + "        LogicalProject(NEW_DEPTNO=[+($7, 1)], EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    ppl =
        "source=EMP | eval NEW_DEPTNO = DEPTNO + 1 | fields NEW_DEPTNO, EMPNO, ENAME, JOB | sort"
            + " NEW_DEPTNO | dedup 1 NEW_DEPTNO";
    root = getRelNode(ppl);
    expectedLogical =
        "LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3])\n"
            + "  LogicalFilter(condition=[<=($4, 1)])\n"
            + "    LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $0)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($0)])\n"
            + "        LogicalSort(sort0=[$0], dir0=[ASC-nulls-first])\n"
            + "          LogicalProject(NEW_DEPTNO=[+($7, 1)], EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testRenameDedup() {
    String ppl =
        "source=EMP | eval TEMP_DEPTNO = DEPTNO + 1 | rename TEMP_DEPTNO as NEW_DEPTNO | fields"
            + " NEW_DEPTNO, EMPNO, ENAME, JOB | dedup 1 NEW_DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3])\n"
            + "  LogicalFilter(condition=[<=($4, 1)])\n"
            + "    LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $0)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($0)])\n"
            + "        LogicalProject(NEW_DEPTNO=[+($7, 1)], EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    ppl =
        "source=EMP | eval TEMP_DEPTNO = DEPTNO + 1 | rename TEMP_DEPTNO as NEW_DEPTNO | fields"
            + " NEW_DEPTNO, EMPNO, ENAME, JOB | dedup 1 JOB";
    root = getRelNode(ppl);
    expectedLogical =
        "LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3])\n"
            + "  LogicalFilter(condition=[<=($4, 1)])\n"
            + "    LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $3)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($3)])\n"
            + "        LogicalProject(NEW_DEPTNO=[+($7, 1)], EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    ppl =
        "source=EMP | eval TEMP_DEPTNO = DEPTNO + 1 | rename TEMP_DEPTNO as NEW_DEPTNO | fields"
            + " NEW_DEPTNO, EMPNO, ENAME, JOB | sort NEW_DEPTNO | dedup 1 NEW_DEPTNO";
    root = getRelNode(ppl);
    expectedLogical =
        "LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3])\n"
            + "  LogicalFilter(condition=[<=($4, 1)])\n"
            + "    LogicalProject(NEW_DEPTNO=[$0], EMPNO=[$1], ENAME=[$2], JOB=[$3],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $0)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($0)])\n"
            + "        LogicalSort(sort0=[$0], dir0=[ASC-nulls-first])\n"
            + "          LogicalProject(NEW_DEPTNO=[+($7, 1)], EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "            LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }
}
