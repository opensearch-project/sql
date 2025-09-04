/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLFillnullTest extends CalcitePPLAbstractTest {

  public CalcitePPLFillnullTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testFillnullWith() {
    String ppl = "source=EMP | fillnull with 0 in MGR, COMM";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[COALESCE($3, 0)], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[COALESCE($6, 0)], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=0;"
            + " DEPTNO=20\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=0; DEPTNO=20\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=0; DEPTNO=30\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=0; DEPTNO=10\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=0; DEPTNO=20\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=0; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=0; DEPTNO=10\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=0; DEPTNO=20\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=0; DEPTNO=30\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=0; DEPTNO=20\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=0; DEPTNO=10\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, COALESCE(`MGR`, 0) `MGR`, `HIREDATE`, `SAL`,"
            + " COALESCE(`COMM`, 0) `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFillnullUsing() {
    String ppl = "source=EMP | fillnull using MGR=7000, COMM=0.0";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[COALESCE($3, 7000)], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[COALESCE($6, 0.0:DECIMAL(2, 1))], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=0.0;"
            + " DEPTNO=20\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=0.0; DEPTNO=20\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=0.0; DEPTNO=30\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=0.0; DEPTNO=10\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=0.0; DEPTNO=20\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=7000; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=0.0; DEPTNO=10\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=0.0; DEPTNO=20\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=0.0; DEPTNO=30\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=0.0; DEPTNO=20\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=0.0; DEPTNO=10\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, COALESCE(`MGR`, 7000) `MGR`, `HIREDATE`, `SAL`,"
            + " COALESCE(`COMM`, 0.0) `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFillnullAll() {
    String ppl = "source=EMP | fields EMPNO, MGR, SAL, COMM, DEPTNO | fillnull with 0";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], MGR=[COALESCE($3, 0)], SAL=[COALESCE($5, 0)],"
            + " COMM=[COALESCE($6, 0)], DEPTNO=[COALESCE($7, 0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; MGR=7902; SAL=800.00; COMM=0; DEPTNO=20\n"
            + "EMPNO=7499; MGR=7698; SAL=1600.00; COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7521; MGR=7698; SAL=1250.00; COMM=500.00; DEPTNO=30\n"
            + "EMPNO=7566; MGR=7839; SAL=2975.00; COMM=0; DEPTNO=20\n"
            + "EMPNO=7654; MGR=7698; SAL=1250.00; COMM=1400.00; DEPTNO=30\n"
            + "EMPNO=7698; MGR=7839; SAL=2850.00; COMM=0; DEPTNO=30\n"
            + "EMPNO=7782; MGR=7839; SAL=2450.00; COMM=0; DEPTNO=10\n"
            + "EMPNO=7788; MGR=7566; SAL=3000.00; COMM=0; DEPTNO=20\n"
            + "EMPNO=7839; MGR=0; SAL=5000.00; COMM=0; DEPTNO=10\n"
            + "EMPNO=7844; MGR=7698; SAL=1500.00; COMM=0.00; DEPTNO=30\n"
            + "EMPNO=7876; MGR=7788; SAL=1100.00; COMM=0; DEPTNO=20\n"
            + "EMPNO=7900; MGR=7698; SAL=950.00; COMM=0; DEPTNO=30\n"
            + "EMPNO=7902; MGR=7566; SAL=3000.00; COMM=0; DEPTNO=20\n"
            + "EMPNO=7934; MGR=7782; SAL=1300.00; COMM=0; DEPTNO=10\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(`MGR`, 0) `MGR`, COALESCE(`SAL`, 0) `SAL`, COALESCE(`COMM`, 0)"
            + " `COMM`, COALESCE(`DEPTNO`, 0) `DEPTNO`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
