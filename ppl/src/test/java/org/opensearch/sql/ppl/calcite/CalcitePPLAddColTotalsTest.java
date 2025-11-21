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
}
