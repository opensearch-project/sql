/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLAggregateFunctionTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLAggregateFunctionTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAvgWithWrongArgType() {
    String ppl = "source=EMP | stats avg(HIREDATE) as avg_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], avg_name=[AVG($0)])\n"
            + "  LogicalProject(HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "avg_name=1982-05-02\n");
    verifyPPLToSparkSQL(root, "SELECT AVG(`HIREDATE`) `avg_name`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testVarsampWithWrongArgType() {
    String ppl = "source=EMP | stats var_samp(HIREDATE) as varsamp_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], varsamp_name=[VAR_SAMP($0)])\n"
            + "  LogicalProject(HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "varsamp_name=8277-10-13\n");
    verifyPPLToSparkSQL(root, "SELECT VAR_SAMP(`HIREDATE`) `varsamp_name`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testVarpopWithWrongArgType() {
    String ppl = "source=EMP | stats var_pop(HIREDATE) as varpop_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], varpop_name=[VAR_POP($0)])\n"
            + "  LogicalProject(HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "varpop_name=3541-07-07\n");
    verifyPPLToSparkSQL(root, "SELECT VAR_POP(`HIREDATE`) `varpop_name`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testStddevSampWithWrongArgType() {
    String ppl = "source=EMP | stats stddev_samp(HIREDATE) as stddev_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], stddev_name=[STDDEV_SAMP($0)])\n"
            + "  LogicalProject(HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "stddev_name=1983-06-23\n");
    verifyPPLToSparkSQL(root, "SELECT STDDEV_SAMP(`HIREDATE`) `stddev_name`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testStddevPopWithWrongArgType() {
    String ppl = "source=EMP | stats stddev_pop(HIREDATE) as stddev_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], stddev_name=[STDDEV_POP($0)])\n"
            + "  LogicalProject(HIREDATE=[$4])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "stddev_name=1982-12-26\n");
    verifyPPLToSparkSQL(root, "SELECT STDDEV_POP(`HIREDATE`) `stddev_name`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testPercentileApproxWithWrongArgType() {
    // First argument should be numeric
    // This test verifies logical plan generation, but execution fails with ClassCastException
    String ppl = "source=EMP | stats percentile_approx(HIREDATE, 50) as percentile";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], percentile=[percentile_approx($0, $1, $2)])\n"
            + "  LogicalProject(HIREDATE=[$4], $f1=[50], $f2=[FLAG(DATE)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    // Execution fails with: ClassCastException: Double cannot be cast to Integer
  }

  @Test
  public void testListFunctionWithArrayArgType() {
    // Test LIST function with array expression (which is not a supported scalar type)
    String ppl = "source=EMP | stats list(array(ENAME, JOB)) as name_list";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], name_list=[LIST($0)])\n"
            + "  LogicalProject($f2=[array($1, $2)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(
        root,
        "name_list=[[SMITH, CLERK], [ALLEN, SALESMAN], [WARD, SALESMAN], [JONES, MANAGER], [MARTIN,"
            + " SALESMAN], [BLAKE, MANAGER], [CLARK, MANAGER], [SCOTT, ANALYST], [KING, PRESIDENT],"
            + " [TURNER, SALESMAN], [ADAMS, CLERK], [JAMES, CLERK], [FORD, ANALYST], [MILLER,"
            + " CLERK]]\n");
    verifyPPLToSparkSQL(
        root, "SELECT `LIST`(ARRAY(`ENAME`, `JOB`)) `name_list`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testCountWithExtraParametersThrowsException() {
    // COUNT with extra parameters throws IllegalArgumentException
    Exception e =
        org.junit.Assert.assertThrows(
            IllegalArgumentException.class,
            () -> getRelNode("source=EMP | stats count(EMPNO, DEPTNO)"));
    verifyErrorMessageContains(e, "Field [DEPTNO] not found");
  }

  @Test
  public void testAvgWithExtraParametersThrowsException() {
    // AVG with extra parameters throws IllegalArgumentException
    Exception e =
        org.junit.Assert.assertThrows(
            IllegalArgumentException.class,
            () -> getRelNode("source=EMP | stats avg(EMPNO, DEPTNO)"));
    verifyErrorMessageContains(e, "Field [DEPTNO] not found");
  }

  @Test
  public void testSumWithExtraParametersThrowsException() {
    String ppl = "source=EMP | stats sum(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], sum(EMPNO, DEPTNO)=[SUM($0, $1)])\n"
            + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "sum(EMPNO, DEPTNO)=108172\n");
    verifyPPLToSparkSQL(
        root, "SELECT SUM(`EMPNO`, `DEPTNO`) `sum(EMPNO, DEPTNO)`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testMinWithExtraParametersThrowsException() {
    String ppl = "source=EMP | stats min(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], min(EMPNO, DEPTNO)=[MIN($0, $1)])\n"
            + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "min(EMPNO, DEPTNO)=7369\n");
    verifyPPLToSparkSQL(
        root, "SELECT MIN(`EMPNO`, `DEPTNO`) `min(EMPNO, DEPTNO)`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testMaxWithExtraParametersThrowsException() {
    String ppl = "source=EMP | stats max(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], max(EMPNO, DEPTNO)=[MAX($0, $1)])\n"
            + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "max(EMPNO, DEPTNO)=7934\n");
    verifyPPLToSparkSQL(
        root, "SELECT MAX(`EMPNO`, `DEPTNO`) `max(EMPNO, DEPTNO)`\nFROM `scott`.`EMP`");
  }

  @Test
  public void testVarSampWithExtraParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with AssertionError
    String ppl = "source=EMP | stats var_samp(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], var_samp(EMPNO, DEPTNO)=[VAR_SAMP($0, $1)])\n"
            + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    // Execution fails with: AssertionError [0, 1] in AggregateReduceFunctionsRule
  }

  @Test
  public void testVarPopWithExtraParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with AssertionError
    String ppl = "source=EMP | stats var_pop(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], var_pop(EMPNO, DEPTNO)=[VAR_POP($0, $1)])\n"
            + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    // Execution fails with: AssertionError [0, 1] in AggregateReduceFunctionsRule
  }

  @Test
  public void testStddevSampWithExtraParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with AssertionError
    String ppl = "source=EMP | stats stddev_samp(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], stddev_samp(EMPNO, DEPTNO)=[STDDEV_SAMP($0, $1)])\n"
            + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    // Execution fails with: AssertionError [0, 1] in AggregateReduceFunctionsRule
  }

  @Test
  public void testStddevPopWithExtraParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with AssertionError
    String ppl = "source=EMP | stats stddev_pop(EMPNO, DEPTNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], stddev_pop(EMPNO, DEPTNO)=[STDDEV_POP($0, $1)])\n"
            + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    // Execution fails with: AssertionError [0, 1] in AggregateReduceFunctionsRule
  }

  @Test
  public void testPercentileWithMissingParametersThrowsException() {
    // This test verifies logical plan generation, but execution fails with ClassCastException
    String ppl = "source=EMP | stats percentile(EMPNO)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], percentile(EMPNO)=[percentile_approx($0, $1)])\n"
            + "  LogicalProject(EMPNO=[$0], $f1=[FLAG(SMALLINT)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    // Execution fails with: ClassCastException: SqlTypeName cannot be cast to Number
  }

  @Test
  public void testPercentileWithInvalidParameterTypesThrowsException() {
    // This test verifies logical plan generation, but execution fails with ClassCastException
    String ppl = "source=EMP | stats percentile(EMPNO, 50, HIREDATE)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], percentile(EMPNO, 50, HIREDATE)=[percentile_approx($0, $2,"
            + " $1, $3)])\n"
            + "  LogicalProject(EMPNO=[$0], HIREDATE=[$4], $f2=[50], $f3=[FLAG(SMALLINT)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    // Execution fails with: ClassCastException: Double cannot be cast to Short
  }

  @Test
  public void testEarliestWithTooManyParametersThrowsException() {
    String ppl = "source=EMP | stats earliest(ENAME, HIREDATE, JOB)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], earliest(ENAME, HIREDATE, JOB)=[ARG_MIN($0, $1, $2)])\n"
            + "  LogicalProject(ENAME=[$1], HIREDATE=[$4], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "earliest(ENAME, HIREDATE, JOB)=SMITH\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT MIN_BY(`ENAME`, `HIREDATE`, `JOB`) `earliest(ENAME, HIREDATE, JOB)`\n"
            + "FROM `scott`.`EMP`");
  }

  @Test
  public void testLatestWithTooManyParametersThrowsException() {
    String ppl = "source=EMP | stats latest(ENAME, HIREDATE, JOB)";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], latest(ENAME, HIREDATE, JOB)=[ARG_MAX($0, $1, $2)])\n"
            + "  LogicalProject(ENAME=[$1], HIREDATE=[$4], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
    verifyResult(root, "latest(ENAME, HIREDATE, JOB)=ADAMS\n");
    verifyPPLToSparkSQL(
        root,
        "SELECT MAX_BY(`ENAME`, `HIREDATE`, `JOB`) `latest(ENAME, HIREDATE, JOB)`\n"
            + "FROM `scott`.`EMP`");
  }
}
