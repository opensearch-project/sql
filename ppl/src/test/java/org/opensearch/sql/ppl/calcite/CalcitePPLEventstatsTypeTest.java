/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLEventstatsTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLEventstatsTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testCountWithExtraParametersThrowsException() {
    // This test verifies logical plan generation for window functions
    // Note: COUNT ignores the second parameter
    String ppl = "source=EMP | eventstats count(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], count(EMPNO, DEPTNO)=[COUNT($0) OVER ()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(
        root,
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; count(EMPNO, DEPTNO)=14\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; count(EMPNO, DEPTNO)=14\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, COUNT(`EMPNO`)"
            + " OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) `count(EMPNO,"
            + " DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
  }

  @Test
  public void testAvgWithExtraParametersThrowsException() {
    // This test verifies logical plan generation for window functions
    // Note: AVG ignores the second parameter
    String ppl = "source=EMP | eventstats avg(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], avg(EMPNO, DEPTNO)=[/(SUM($0) OVER (), CAST(COUNT($0) OVER"
            + " ()):DOUBLE NOT NULL)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(
        root,
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; avg(EMPNO, DEPTNO)=7726.571428571428\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; avg(EMPNO, DEPTNO)=7726.571428571428\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, (SUM(`EMPNO`)"
            + " OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) /"
            + " CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE) `avg(EMPNO, DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSumWithExtraParametersThrowsException() {
    // This test verifies logical plan generation for window functions
    String ppl = "source=EMP | eventstats sum(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], sum(EMPNO, DEPTNO)=[SUM($0, $7) OVER ()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(
        root,
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; sum(EMPNO, DEPTNO)=108172\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; sum(EMPNO, DEPTNO)=108172\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, SUM(`EMPNO`,"
            + " `DEPTNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `sum(EMPNO, DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
  }

  @Test
  public void testMinWithExtraParametersThrowsException() {
    // This test verifies logical plan generation for window functions
    String ppl = "source=EMP | eventstats min(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], min(EMPNO, DEPTNO)=[MIN($0, $7) OVER ()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(
        root,
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; min(EMPNO, DEPTNO)=7369\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; min(EMPNO, DEPTNO)=7369\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, MIN(`EMPNO`,"
            + " `DEPTNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `min(EMPNO, DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
  }

  @Test
  public void testMaxWithExtraParametersThrowsException() {
    // This test verifies logical plan generation for window functions
    String ppl = "source=EMP | eventstats max(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], max(EMPNO, DEPTNO)=[MAX($0, $7) OVER ()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(
        root,
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; max(EMPNO, DEPTNO)=7934\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; max(EMPNO, DEPTNO)=7934\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, MAX(`EMPNO`,"
            + " `DEPTNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            + " `max(EMPNO, DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
  }

  @Test
  public void testVarSampWithExtraParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with ArithmeticException
    String ppl = "source=EMP | eventstats var_samp(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], var_samp(EMPNO, DEPTNO)=[/(-(SUM(*($0, $0)) OVER (),"
            + " /(*(SUM($0) OVER (), SUM($0) OVER ()), CAST(COUNT($0) OVER ()):DOUBLE NOT NULL)),"
            + " -(CAST(COUNT($0) OVER ()):DOUBLE NOT NULL, 1))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, ((SUM(`EMPNO`"
            + " * `EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) -"
            + " (SUM(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) *"
            + " (SUM(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) /"
            + " CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE)) / (CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING) AS DOUBLE) - 1) `var_samp(EMPNO, DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
    // Execution fails with: ArithmeticException: Value out of range
    Exception e =
        org.junit.Assert.assertThrows(RuntimeException.class, () -> verifyResultCount(root, 0));
    verifyErrorMessageContains(e, "out of range");
  }

  @Test
  public void testVarPopWithExtraParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with ArithmeticException
    String ppl = "source=EMP | eventstats var_pop(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], var_pop(EMPNO, DEPTNO)=[/(-(SUM(*($0, $0)) OVER (),"
            + " /(*(SUM($0) OVER (), SUM($0) OVER ()), CAST(COUNT($0) OVER ()):DOUBLE NOT NULL)),"
            + " CAST(COUNT($0) OVER ()):DOUBLE NOT NULL)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, ((SUM(`EMPNO`"
            + " * `EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) -"
            + " (SUM(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) *"
            + " (SUM(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) /"
            + " CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED"
            + " FOLLOWING) AS DOUBLE)) / CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED"
            + " PRECEDING AND UNBOUNDED FOLLOWING) AS DOUBLE) `var_pop(EMPNO, DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
    // Execution fails with: ArithmeticException: Value out of range
    Exception e =
        org.junit.Assert.assertThrows(RuntimeException.class, () -> verifyResultCount(root, 0));
    verifyErrorMessageContains(e, "out of range");
  }

  @Test
  public void testStddevSampWithExtraParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with ArithmeticException
    String ppl = "source=EMP | eventstats stddev_samp(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], stddev_samp(EMPNO, DEPTNO)=[POWER(/(-(SUM(*($0, $0)) OVER"
            + " (), /(*(SUM($0) OVER (), SUM($0) OVER ()), CAST(COUNT($0) OVER ()):DOUBLE NOT"
            + " NULL)), -(CAST(COUNT($0) OVER ()):DOUBLE NOT NULL, 1)), 0.5E0:DOUBLE)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " POWER(((SUM(`EMPNO` * `EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING)) - (SUM(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING)) * (SUM(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING)) / CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING"
            + " AND UNBOUNDED FOLLOWING) AS DOUBLE)) / (CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DOUBLE) - 1), 5E-1)"
            + " `stddev_samp(EMPNO, DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
    // Execution fails with: ArithmeticException: Value out of range
    Exception e =
        org.junit.Assert.assertThrows(RuntimeException.class, () -> verifyResultCount(root, 0));
    verifyErrorMessageContains(e, "out of range");
  }

  @Test
  public void testStddevPopWithExtraParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with ArithmeticException
    String ppl = "source=EMP | eventstats stddev_pop(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], stddev_pop(EMPNO, DEPTNO)=[POWER(/(-(SUM(*($0, $0)) OVER"
            + " (), /(*(SUM($0) OVER (), SUM($0) OVER ()), CAST(COUNT($0) OVER ()):DOUBLE NOT"
            + " NULL)), CAST(COUNT($0) OVER ()):DOUBLE NOT NULL), 0.5E0:DOUBLE)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " POWER(((SUM(`EMPNO` * `EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING)) - (SUM(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING)) * (SUM(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING)) / CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING"
            + " AND UNBOUNDED FOLLOWING) AS DOUBLE)) / CAST(COUNT(`EMPNO`) OVER (RANGE BETWEEN"
            + " UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS DOUBLE), 5E-1) `stddev_pop(EMPNO,"
            + " DEPTNO)`\n"
            + "FROM `scott`.`EMP`");
    // Execution fails with: ArithmeticException: Value out of range
    Exception e =
        org.junit.Assert.assertThrows(RuntimeException.class, () -> verifyResultCount(root, 0));
    verifyErrorMessageContains(e, "out of range");
  }

  @Test
  public void testEarliestWithTooManyParametersThrowsException() {
    // This test verifies logical plan generation for window functions
    String ppl = "source=EMP | eventstats earliest(ENAME, HIREDATE, JOB)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], earliest(ENAME, HIREDATE, JOB)=[ARG_MIN($1, $4, $2) OVER"
            + " ()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(
        root,
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; earliest(ENAME, HIREDATE, JOB)=SMITH\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; earliest(ENAME, HIREDATE, JOB)=SMITH\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " MIN_BY(`ENAME`, `HIREDATE`, `JOB`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING) `earliest(ENAME, HIREDATE, JOB)`\n"
            + "FROM `scott`.`EMP`");
  }

  @Test
  public void testLatestWithTooManyParametersThrowsException() {
    // This test verifies logical plan generation for window functions
    String ppl = "source=EMP | eventstats latest(ENAME, HIREDATE, JOB)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], latest(ENAME, HIREDATE, JOB)=[ARG_MAX($1, $4, $2) OVER"
            + " ()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(
        root,
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; latest(ENAME, HIREDATE, JOB)=ADAMS\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; latest(ENAME, HIREDATE, JOB)=ADAMS\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " MAX_BY(`ENAME`, `HIREDATE`, `JOB`) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND"
            + " UNBOUNDED FOLLOWING) `latest(ENAME, HIREDATE, JOB)`\n"
            + "FROM `scott`.`EMP`");
  }

  @Test
  public void testAvgWithWrongArgType() {
    // AVG with DATE argument throws CalciteContextException during plan generation
    String ppl = "source=EMP | eventstats avg(HIREDATE) as avg_name";
    Exception e =
        org.junit.Assert.assertThrows(
            org.apache.calcite.runtime.CalciteContextException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "Cannot infer return type for /; operand types: [DATE, DOUBLE]");
  }

  @Test
  public void testVarsampWithWrongArgType() {
    // VAR_SAMP with DATE argument throws CalciteContextException during plan generation
    String ppl = "source=EMP | eventstats var_samp(HIREDATE) as varsamp_name";
    Exception e =
        org.junit.Assert.assertThrows(
            org.apache.calcite.runtime.CalciteContextException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "Cannot infer return type for /; operand types: [DATE, DOUBLE]");
  }

  @Test
  public void testVarpopWithWrongArgType() {
    // VAR_POP with DATE argument throws CalciteContextException during plan generation
    String ppl = "source=EMP | eventstats var_pop(HIREDATE) as varpop_name";
    Exception e =
        org.junit.Assert.assertThrows(
            org.apache.calcite.runtime.CalciteContextException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "Cannot infer return type for /; operand types: [DATE, DOUBLE]");
  }

  @Test
  public void testStddevSampWithWrongArgType() {
    // STDDEV_SAMP with DATE argument throws CalciteContextException during plan generation
    String ppl = "source=EMP | eventstats stddev_samp(HIREDATE) as stddev_name";
    Exception e =
        org.junit.Assert.assertThrows(
            org.apache.calcite.runtime.CalciteContextException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "Cannot infer return type for /; operand types: [DATE, DOUBLE]");
  }

  @Test
  public void testStddevPopWithWrongArgType() {
    // STDDEV_POP with DATE argument throws CalciteContextException during plan generation
    String ppl = "source=EMP | eventstats stddev_pop(HIREDATE) as stddev_name";
    Exception e =
        org.junit.Assert.assertThrows(
            org.apache.calcite.runtime.CalciteContextException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "Cannot infer return type for /; operand types: [DATE, DOUBLE]");
  }
}
