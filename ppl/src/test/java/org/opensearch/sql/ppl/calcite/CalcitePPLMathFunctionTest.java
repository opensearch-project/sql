/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLMathFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLMathFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAbsWithOverriding() {
    String ppl = "source=EMP | eval SAL = abs(-30) | head 10 | fields SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SAL0=[$7])\n"
            + "  LogicalSort(fetch=[10])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " COMM=[$6], DEPTNO=[$7], SAL0=[ABS(-30)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult =
        "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n"
            + "SAL0=30\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT ABS(-30) `SAL0`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSqrtWithOverriding() {
    String ppl = "source=EMP | eval SQRT = sqrt(4) | head 2 | fields SQRT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SQRT=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], SQRT=[SQRT(4)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "SQRT=2.0\n" + "SQRT=2.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT SQRT(4) `SQRT`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAtanWithOverriding() {
    String ppl = "source=EMP | eval ATAN = atan(2) | head 2 | fields ATAN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ATAN=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], ATAN=[ATAN2(2, 1:BIGINT)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "ATAN=1.1071487177940904\n" + "ATAN=1.1071487177940904\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT ATAN2(2, 1) `ATAN`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAtan2WithOverriding() {
    String ppl = "source=EMP | eval ATAN2 = atan(2, 3) | head 2 | fields ATAN";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ATAN2=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], ATAN2=[ATAN2(2, 3)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "ATAN2=0.5880026035475675\n" + "ATAN2=0.5880026035475675\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT ATAN2(2, 3) `ATAN2`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testPowWithOverriding() {
    String ppl = "source=EMP | eval POW = POW(3, 2) | head 2 | fields POW";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(POW=[$8])\n"
            + "  LogicalSort(fetch=[2])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], POW=[POWER(3, 2)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "POW=9.0\n" + "POW=9.0\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "" + "SELECT POWER(3, 2) `POW`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
