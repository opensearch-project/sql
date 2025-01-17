/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class CalcitePPLLookupTest extends CalcitePPLAbstractTest {

  public CalcitePPLLookupTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testReplace() {
    String ppl = "source=EMP | lookup DEPT DEPTNO replace LOC";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], LOC=[$9])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(DEPTNO=[$0], LOC=[$2])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; LOC=NEW YORK\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; LOC=NEW YORK\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; LOC=NEW YORK\n"
            + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; LOC=CHICAGO\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `t`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN (SELECT `DEPTNO`, `LOC`\n"
            + "FROM `scott`.`DEPT`) `t` ON `EMP`.`DEPTNO` = `t`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testReplaceAs() {
    String ppl = "source=EMP | lookup DEPT DEPTNO replace LOC as JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6],"
            + " DEPTNO=[$7], JOB=[COALESCE($9, $2)])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(DEPTNO=[$0], LOC=[$2])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7782; ENAME=CLARK; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00; COMM=null; DEPTNO=10;"
            + " JOB=NEW YORK\n"
            + "EMPNO=7839; ENAME=KING; MGR=null; HIREDATE=1981-11-17; SAL=5000.00; COMM=null;"
            + " DEPTNO=10; JOB=NEW YORK\n"
            + "EMPNO=7934; ENAME=MILLER; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00; COMM=null;"
            + " DEPTNO=10; JOB=NEW YORK\n"
            + "EMPNO=7369; ENAME=SMITH; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; JOB=DALLAS\n"
            + "EMPNO=7566; ENAME=JONES; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00; COMM=null;"
            + " DEPTNO=20; JOB=DALLAS\n"
            + "EMPNO=7788; ENAME=SCOTT; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00; COMM=null;"
            + " DEPTNO=20; JOB=DALLAS\n"
            + "EMPNO=7876; ENAME=ADAMS; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00; COMM=null;"
            + " DEPTNO=20; JOB=DALLAS\n"
            + "EMPNO=7902; ENAME=FORD; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00; COMM=null;"
            + " DEPTNO=20; JOB=DALLAS\n"
            + "EMPNO=7499; ENAME=ALLEN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00; COMM=300.00;"
            + " DEPTNO=30; JOB=CHICAGO\n"
            + "EMPNO=7521; ENAME=WARD; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00; COMM=500.00;"
            + " DEPTNO=30; JOB=CHICAGO\n"
            + "EMPNO=7654; ENAME=MARTIN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00; COMM=1400.00;"
            + " DEPTNO=30; JOB=CHICAGO\n"
            + "EMPNO=7698; ENAME=BLAKE; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00; COMM=null;"
            + " DEPTNO=30; JOB=CHICAGO\n"
            + "EMPNO=7844; ENAME=TURNER; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00; COMM=0.00;"
            + " DEPTNO=30; JOB=CHICAGO\n"
            + "EMPNO=7900; ENAME=JAMES; MGR=7698; HIREDATE=1981-12-03; SAL=950.00; COMM=null;"
            + " DEPTNO=30; JOB=CHICAGO\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`MGR`, `EMP`.`HIREDATE`, `EMP`.`SAL`,"
            + " `EMP`.`COMM`, `EMP`.`DEPTNO`, COALESCE(`t`.`LOC`, `EMP`.`JOB`) `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN (SELECT `DEPTNO`, `LOC`\n"
            + "FROM `scott`.`DEPT`) `t` ON `EMP`.`DEPTNO` = `t`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Ignore
  public void testMultipleLookupKeysReplace() {
    String ppl =
        "source=EMP | eval newNO = DEPTNO | lookup DEPT DEPTNO as newNO, DEPTNO replace LOC as JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "";
    verifyResult(root, expectedResult);
  }

  @Test
  public void testAppend() {
    String ppl = "source=EMP | lookup DEPT DEPTNO append LOC";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], LOC=[$9])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(DEPTNO=[$0], LOC=[$2])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; LOC=NEW YORK\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; LOC=NEW YORK\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; LOC=NEW YORK\n"
            + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; LOC=DALLAS\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; LOC=CHICAGO\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; LOC=CHICAGO\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `t`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN (SELECT `DEPTNO`, `LOC`\n"
            + "FROM `scott`.`DEPT`) `t` ON `EMP`.`DEPTNO` = `t`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendAs() {
    String ppl = "source=EMP | lookup DEPT DEPTNO append LOC as JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6],"
            + " DEPTNO=[$7], JOB=[COALESCE(COALESCE($2, $9), $2)])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(DEPTNO=[$0], LOC=[$2])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7782; ENAME=CLARK; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00; COMM=null; DEPTNO=10;"
            + " JOB=MANAGER\n"
            + "EMPNO=7839; ENAME=KING; MGR=null; HIREDATE=1981-11-17; SAL=5000.00; COMM=null;"
            + " DEPTNO=10; JOB=PRESIDENT\n"
            + "EMPNO=7934; ENAME=MILLER; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00; COMM=null;"
            + " DEPTNO=10; JOB=CLERK\n"
            + "EMPNO=7369; ENAME=SMITH; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20; JOB=CLERK\n"
            + "EMPNO=7566; ENAME=JONES; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00; COMM=null;"
            + " DEPTNO=20; JOB=MANAGER\n"
            + "EMPNO=7788; ENAME=SCOTT; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00; COMM=null;"
            + " DEPTNO=20; JOB=ANALYST\n"
            + "EMPNO=7876; ENAME=ADAMS; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00; COMM=null;"
            + " DEPTNO=20; JOB=CLERK\n"
            + "EMPNO=7902; ENAME=FORD; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00; COMM=null;"
            + " DEPTNO=20; JOB=ANALYST\n"
            + "EMPNO=7499; ENAME=ALLEN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00; COMM=300.00;"
            + " DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7521; ENAME=WARD; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00; COMM=500.00;"
            + " DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7654; ENAME=MARTIN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00; COMM=1400.00;"
            + " DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7698; ENAME=BLAKE; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00; COMM=null;"
            + " DEPTNO=30; JOB=MANAGER\n"
            + "EMPNO=7844; ENAME=TURNER; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00; COMM=0.00;"
            + " DEPTNO=30; JOB=SALESMAN\n"
            + "EMPNO=7900; ENAME=JAMES; MGR=7698; HIREDATE=1981-12-03; SAL=950.00; COMM=null;"
            + " DEPTNO=30; JOB=CLERK\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`MGR`, `EMP`.`HIREDATE`, `EMP`.`SAL`,"
            + " `EMP`.`COMM`, `EMP`.`DEPTNO`, COALESCE(COALESCE(`EMP`.`JOB`, `t`.`LOC`),"
            + " `EMP`.`JOB`) `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN (SELECT `DEPTNO`, `LOC`\n"
            + "FROM `scott`.`DEPT`) `t` ON `EMP`.`DEPTNO` = `t`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Ignore
  public void testMultipleLookupKeysAppend() {
    String ppl =
        "source=EMP | eval newNO = DEPTNO | lookup DEPT DEPTNO as newNO, DEPTNO append LOC as COMM";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testLookupAll() {
    String ppl = "source=EMP | lookup DEPT DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult =
        "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10; DEPTNO0=10; DNAME=ACCOUNTING; LOC=NEW YORK\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10; DEPTNO0=10; DNAME=ACCOUNTING; LOC=NEW YORK\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10; DEPTNO0=10; DNAME=ACCOUNTING; LOC=NEW YORK\n"
            + "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00;"
            + " COMM=null; DEPTNO=20; DEPTNO0=20; DNAME=RESEARCH; LOC=DALLAS\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20; DEPTNO0=20; DNAME=RESEARCH; LOC=DALLAS\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; DEPTNO0=20; DNAME=RESEARCH; LOC=DALLAS\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20; DEPTNO0=20; DNAME=RESEARCH; LOC=DALLAS\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20; DEPTNO0=20; DNAME=RESEARCH; LOC=DALLAS\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30; DEPTNO0=30; DNAME=SALES; LOC=CHICAGO\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30; DEPTNO0=30; DNAME=SALES; LOC=CHICAGO\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30; DEPTNO0=30; DNAME=SALES; LOC=CHICAGO\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30; DEPTNO0=30; DNAME=SALES; LOC=CHICAGO\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30; DEPTNO0=30; DNAME=SALES; LOC=CHICAGO\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30; DEPTNO0=30; DNAME=SALES; LOC=CHICAGO\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
