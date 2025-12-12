/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.io.IOException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLAddColTotalsTest extends CalcitePPLAbstractTest {

  public CalcitePPLAddColTotalsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAddColTotals() throws IOException {
    String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addcoltotals ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[$0], SAL=[$1], JOB=[null:VARCHAR(9)])\n"
            + "    LogicalAggregate(group=[{}], DEPTNO=[SUM($0)], SAL=[SUM($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "DEPTNO=20; SAL=800.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=1600.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=2975.00; JOB=MANAGER\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=2850.00; JOB=MANAGER\n"
            + "DEPTNO=10; SAL=2450.00; JOB=MANAGER\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=5000.00; JOB=PRESIDENT\n"
            + "DEPTNO=30; SAL=1500.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=1100.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=950.00; JOB=CLERK\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=1300.00; JOB=CLERK\n"
            + "DEPTNO=310; SAL=29025.00; JOB=null\n";

    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `SAL`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT SUM(`DEPTNO`) `DEPTNO`, SUM(`SAL`) `SAL`, CAST(NULL AS STRING) `JOB`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAddColTotalsFieldSpecified() throws IOException {
    String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addcoltotals SAL ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[null:TINYINT], SAL=[$0], JOB=[null:VARCHAR(9)])\n"
            + "    LogicalAggregate(group=[{}], SAL=[SUM($0)])\n"
            + "      LogicalProject(SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "DEPTNO=20; SAL=800.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=1600.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=2975.00; JOB=MANAGER\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=2850.00; JOB=MANAGER\n"
            + "DEPTNO=10; SAL=2450.00; JOB=MANAGER\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=5000.00; JOB=PRESIDENT\n"
            + "DEPTNO=30; SAL=1500.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=1100.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=950.00; JOB=CLERK\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=1300.00; JOB=CLERK\n"
            + "DEPTNO=null; SAL=29025.00; JOB=null\n";

    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `SAL`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT CAST(NULL AS TINYINT) `DEPTNO`, SUM(`SAL`) `SAL`, CAST(NULL AS STRING)"
            + " `JOB`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAddColTotalsAllFields() throws IOException {
    String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addcoltotals  ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[$0], SAL=[$1], JOB=[null:VARCHAR(9)])\n"
            + "    LogicalAggregate(group=[{}], DEPTNO=[SUM($0)], SAL=[SUM($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "DEPTNO=20; SAL=800.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=1600.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=2975.00; JOB=MANAGER\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=2850.00; JOB=MANAGER\n"
            + "DEPTNO=10; SAL=2450.00; JOB=MANAGER\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=5000.00; JOB=PRESIDENT\n"
            + "DEPTNO=30; SAL=1500.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=1100.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=950.00; JOB=CLERK\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=1300.00; JOB=CLERK\n"
            + "DEPTNO=310; SAL=29025.00; JOB=null\n";

    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `SAL`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT SUM(`DEPTNO`) `DEPTNO`, SUM(`SAL`) `SAL`, CAST(NULL AS STRING) `JOB`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAddColTotalsMultiFields() throws IOException {
    String ppl = "source=EMP  | fields DEPTNO, SAL, JOB | addcoltotals DEPTNO SAL ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[$0], SAL=[$1], JOB=[null:VARCHAR(9)])\n"
            + "    LogicalAggregate(group=[{}], DEPTNO=[SUM($0)], SAL=[SUM($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "DEPTNO=20; SAL=800.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=1600.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=2975.00; JOB=MANAGER\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=2850.00; JOB=MANAGER\n"
            + "DEPTNO=10; SAL=2450.00; JOB=MANAGER\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=5000.00; JOB=PRESIDENT\n"
            + "DEPTNO=30; SAL=1500.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=1100.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=950.00; JOB=CLERK\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=1300.00; JOB=CLERK\n"
            + "DEPTNO=310; SAL=29025.00; JOB=null\n";

    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `SAL`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT SUM(`DEPTNO`) `DEPTNO`, SUM(`SAL`) `SAL`, CAST(NULL AS STRING) `JOB`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAddColTotalsWithAllOptions() throws IOException {
    String ppl =
        "source=EMP  | fields DEPTNO, SAL, JOB | addcoltotals SAL label='GrandTotal'"
            + " labelfield='all_emp_total' ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2],"
            + " all_emp_total=[null:VARCHAR(13)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[null:TINYINT], SAL=[$0], JOB=[null:VARCHAR(9)],"
            + " all_emp_total=['GrandTotal':VARCHAR(13)])\n"
            + "    LogicalAggregate(group=[{}], SAL=[SUM($0)])\n"
            + "      LogicalProject(SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "DEPTNO=20; SAL=800.00; JOB=CLERK; all_emp_total=null\n"
            + "DEPTNO=30; SAL=1600.00; JOB=SALESMAN; all_emp_total=null\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN; all_emp_total=null\n"
            + "DEPTNO=20; SAL=2975.00; JOB=MANAGER; all_emp_total=null\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN; all_emp_total=null\n"
            + "DEPTNO=30; SAL=2850.00; JOB=MANAGER; all_emp_total=null\n"
            + "DEPTNO=10; SAL=2450.00; JOB=MANAGER; all_emp_total=null\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST; all_emp_total=null\n"
            + "DEPTNO=10; SAL=5000.00; JOB=PRESIDENT; all_emp_total=null\n"
            + "DEPTNO=30; SAL=1500.00; JOB=SALESMAN; all_emp_total=null\n"
            + "DEPTNO=20; SAL=1100.00; JOB=CLERK; all_emp_total=null\n"
            + "DEPTNO=30; SAL=950.00; JOB=CLERK; all_emp_total=null\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST; all_emp_total=null\n"
            + "DEPTNO=10; SAL=1300.00; JOB=CLERK; all_emp_total=null\n"
            + "DEPTNO=null; SAL=29025.00; JOB=null; all_emp_total=GrandTotal\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `SAL`, `JOB`, CAST(NULL AS STRING) `all_emp_total`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT CAST(NULL AS TINYINT) `DEPTNO`, SUM(`SAL`) `SAL`, CAST(NULL AS STRING) `JOB`,"
            + " 'GrandTotal' `all_emp_total`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAddColTotalsMatchingLabelFieldWithExisting() throws IOException {
    String ppl =
        "source=EMP  | fields DEPTNO, SAL, JOB | addcoltotals SAL label='GrandTotal'"
            + " labelfield='JOB' ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[null:TINYINT], SAL=[$0], JOB=['GrandTota':VARCHAR(9)])\n"
            + "    LogicalAggregate(group=[{}], SAL=[SUM($0)])\n"
            + "      LogicalProject(SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "DEPTNO=20; SAL=800.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=1600.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=2975.00; JOB=MANAGER\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=2850.00; JOB=MANAGER\n"
            + "DEPTNO=10; SAL=2450.00; JOB=MANAGER\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=5000.00; JOB=PRESIDENT\n"
            + "DEPTNO=30; SAL=1500.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=1100.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=950.00; JOB=CLERK\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=1300.00; JOB=CLERK\n"
            + "DEPTNO=null; SAL=29025.00; JOB=GrandTota\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `SAL`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT CAST(NULL AS TINYINT) `DEPTNO`, SUM(`SAL`) `SAL`, 'GrandTota' `JOB`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAddColTotalsMatchingLabelFieldWithExistingChangedOrder() throws IOException {
    String ppl =
        "source=EMP  | fields DEPTNO, SAL, JOB | addcoltotals label='GrandTotal'"
            + " labelfield='JOB' SAL ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], SAL=[$5], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[null:TINYINT], SAL=[$0], JOB=['GrandTota':VARCHAR(9)])\n"
            + "    LogicalAggregate(group=[{}], SAL=[SUM($0)])\n"
            + "      LogicalProject(SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "DEPTNO=20; SAL=800.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=1600.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=2975.00; JOB=MANAGER\n"
            + "DEPTNO=30; SAL=1250.00; JOB=SALESMAN\n"
            + "DEPTNO=30; SAL=2850.00; JOB=MANAGER\n"
            + "DEPTNO=10; SAL=2450.00; JOB=MANAGER\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=5000.00; JOB=PRESIDENT\n"
            + "DEPTNO=30; SAL=1500.00; JOB=SALESMAN\n"
            + "DEPTNO=20; SAL=1100.00; JOB=CLERK\n"
            + "DEPTNO=30; SAL=950.00; JOB=CLERK\n"
            + "DEPTNO=20; SAL=3000.00; JOB=ANALYST\n"
            + "DEPTNO=10; SAL=1300.00; JOB=CLERK\n"
            + "DEPTNO=null; SAL=29025.00; JOB=GrandTota\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `SAL`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT CAST(NULL AS TINYINT) `DEPTNO`, SUM(`SAL`) `SAL`, 'GrandTota' `JOB`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAddColTotalsAllFieldsWithLabel() throws IOException {
    String ppl = "source=EMP  | addcoltotals label='GrandTotal' " + " labelfield='JOB' ";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[null:VARCHAR(10)], JOB=['GrandTota':VARCHAR(9)],"
            + " MGR=[$1], HIREDATE=[null:DATE], SAL=[$2], COMM=[$3], DEPTNO=[$4])\n"
            + "    LogicalAggregate(group=[{}], EMPNO=[SUM($0)], MGR=[SUM($3)], SAL=[SUM($5)],"
            + " COMM=[SUM($6)], DEPTNO=[SUM($7)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
    String expectedResult =
        "EMPNO=7369; ENAME=SMITH; JOB=CLERK; MGR=7902; HIREDATE=1980-12-17; SAL=800.00; COMM=null;"
            + " DEPTNO=20\n"
            + "EMPNO=7499; ENAME=ALLEN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-20; SAL=1600.00;"
            + " COMM=300.00; DEPTNO=30\n"
            + "EMPNO=7521; ENAME=WARD; JOB=SALESMAN; MGR=7698; HIREDATE=1981-02-22; SAL=1250.00;"
            + " COMM=500.00; DEPTNO=30\n"
            + "EMPNO=7566; ENAME=JONES; JOB=MANAGER; MGR=7839; HIREDATE=1981-02-04; SAL=2975.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7654; ENAME=MARTIN; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-28; SAL=1250.00;"
            + " COMM=1400.00; DEPTNO=30\n"
            + "EMPNO=7698; ENAME=BLAKE; JOB=MANAGER; MGR=7839; HIREDATE=1981-01-05; SAL=2850.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7782; ENAME=CLARK; JOB=MANAGER; MGR=7839; HIREDATE=1981-06-09; SAL=2450.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7788; ENAME=SCOTT; JOB=ANALYST; MGR=7566; HIREDATE=1987-04-19; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7839; ENAME=KING; JOB=PRESIDENT; MGR=null; HIREDATE=1981-11-17; SAL=5000.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=7844; ENAME=TURNER; JOB=SALESMAN; MGR=7698; HIREDATE=1981-09-08; SAL=1500.00;"
            + " COMM=0.00; DEPTNO=30\n"
            + "EMPNO=7876; ENAME=ADAMS; JOB=CLERK; MGR=7788; HIREDATE=1987-05-23; SAL=1100.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7900; ENAME=JAMES; JOB=CLERK; MGR=7698; HIREDATE=1981-12-03; SAL=950.00;"
            + " COMM=null; DEPTNO=30\n"
            + "EMPNO=7902; ENAME=FORD; JOB=ANALYST; MGR=7566; HIREDATE=1981-12-03; SAL=3000.00;"
            + " COMM=null; DEPTNO=20\n"
            + "EMPNO=7934; ENAME=MILLER; JOB=CLERK; MGR=7782; HIREDATE=1982-01-23; SAL=1300.00;"
            + " COMM=null; DEPTNO=10\n"
            + "EMPNO=108172; ENAME=null; JOB=GrandTota; MGR=100611; HIREDATE=null; SAL=29025.00;"
            + " COMM=2200.00; DEPTNO=310\n";

    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT SUM(`EMPNO`) `EMPNO`, CAST(NULL AS STRING) `ENAME`, 'GrandTota' `JOB`,"
            + " SUM(`MGR`) `MGR`, CAST(NULL AS DATE) `HIREDATE`, SUM(`SAL`) `SAL`, SUM(`COMM`)"
            + " `COMM`, SUM(`DEPTNO`) `DEPTNO`\n"
            + "FROM `scott`.`EMP`";

    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
