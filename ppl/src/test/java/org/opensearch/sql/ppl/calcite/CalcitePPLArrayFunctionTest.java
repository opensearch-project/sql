/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLArrayFunctionTest extends CalcitePPLAbstractTest {

  public CalcitePPLArrayFunctionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testMvjoinWithStringArray() {
    String ppl =
        "source=EMP | eval joined = mvjoin(array('a', 'b', 'c'), ',') | head 1 | fields joined";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(joined=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], joined=[ARRAY_JOIN(array('a', 'b', 'c'), ',')])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "joined=a,b,c\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARRAY_JOIN(`array`('a', 'b', 'c'), ',') `joined`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvjoinWithDifferentDelimiter() {
    String ppl =
        "source=EMP | eval joined = mvjoin(array('apple', 'banana', 'cherry'), ' | ') | head 1 |"
            + " fields joined";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(joined=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], joined=[ARRAY_JOIN(array('apple':VARCHAR,"
            + " 'banana':VARCHAR, 'cherry':VARCHAR), ' | ':VARCHAR)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "joined=apple | banana | cherry\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARRAY_JOIN(`array`('apple', 'banana', 'cherry'), ' | ') `joined`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvjoinWithEmptyArray() {
    String ppl = "source=EMP | eval joined = mvjoin(array(), ',') | head 1 | fields joined";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(joined=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], joined=[ARRAY_JOIN(array(), ',')])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "joined=\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARRAY_JOIN(`array`(), ',') `joined`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvjoinWithFieldReference() {
    String ppl =
        "source=EMP | eval joined = mvjoin(array(ENAME, JOB), '-') | head 1 | fields joined";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(joined=[$8])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], joined=[ARRAY_JOIN(array($1, $2), '-')])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT ARRAY_JOIN(`array`(`ENAME`, `JOB`), '-') `joined`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvindexSingleElementPositive() {
    String ppl =
        "source=EMP | eval arr = array('a', 'b', 'c'), result = mvindex(arr, 1) | head 1 |"
            + " fields result";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(result=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[array('a', 'b', 'c')],"
            + " result=[ITEM(array('a', 'b', 'c'), +(1, 1))])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "result=b\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `array`('a', 'b', 'c')[1 + 1] `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvindexSingleElementNegative() {
    String ppl =
        "source=EMP | eval arr = array('a', 'b', 'c'), result = mvindex(arr, -1) | head 1 |"
            + " fields result";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(result=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[array('a', 'b', 'c')],"
            + " result=[ITEM(array('a', 'b', 'c'), +(+(ARRAY_LENGTH(array('a', 'b', 'c')),"
            + " -1), 1))])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "result=c\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT `array`('a', 'b', 'c')[ARRAY_LENGTH(`array`('a', 'b', 'c')) + -1 + 1]"
            + " `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvindexRangePositive() {
    String ppl =
        "source=EMP | eval arr = array(1, 2, 3, 4, 5), result = mvindex(arr, 1, 3) | head 1 |"
            + " fields result";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(result=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[array(1, 2, 3, 4, 5)],"
            + " result=[ARRAY_SLICE(array(1, 2, 3, 4, 5), 1, +(-(3, 1), 1))])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "result=[2, 3, 4]\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARRAY_SLICE(`array`(1, 2, 3, 4, 5), 1, 3 - 1 + 1) `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMvindexRangeNegative() {
    String ppl =
        "source=EMP | eval arr = array(1, 2, 3, 4, 5), result = mvindex(arr, -3, -1) | head 1 |"
            + " fields result";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(result=[$9])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], arr=[array(1, 2, 3, 4, 5)],"
            + " result=[ARRAY_SLICE(array(1, 2, 3, 4, 5), +(ARRAY_LENGTH(array(1, 2, 3, 4, 5)),"
            + " -3), +(-(+(ARRAY_LENGTH(array(1, 2, 3, 4, 5)), -1),"
            + " +(ARRAY_LENGTH(array(1, 2, 3, 4, 5)), -3)), 1))])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedResult = "result=[3, 4, 5]\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        "SELECT ARRAY_SLICE(`array`(1, 2, 3, 4, 5), ARRAY_LENGTH(`array`(1, 2, 3, 4, 5)) + -3,"
            + " ARRAY_LENGTH(`array`(1, 2, 3, 4, 5)) + -1 - (ARRAY_LENGTH(`array`(1, 2, 3, 4, 5))"
            + " + -3) + 1) `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
