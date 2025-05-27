/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class CalcitePPLFunctionTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLFunctionTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testLowerWithIntegerType() {
    String ppl = "source=EMP | eval lower_name = lower(EMPNO) | fields lower_name";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(t, "LOWER function expects {[STRING]}, but got [SHORT]");
  }

  @Test
  public void testTimeDiffWithUdtInputType() {
    String strPpl =
        "source=EMP | eval time_diff = timediff('12:00:00', '12:00:06') | fields time_diff";
    String timePpl =
        "source=EMP | eval time_diff = timediff(time('13:00:00'), time('12:00:06')) | fields"
            + " time_diff";
    String wrongPpl = "source=EMP | eval time_diff = timediff(12, '2009-12-10') | fields time_diff";
    getRelNode(strPpl);
    getRelNode(timePpl);
    Throwable t = Assert.assertThrows(Exception.class, () -> getRelNode(wrongPpl));
    verifyErrorMessageContains(
        t,
        "TIMEDIFF function expects"
            + " {[STRING,STRING],[DATE,DATE],[DATE,TIME],[DATE,TIMESTAMP],[TIME,DATE],[TIME,TIME],[TIME,TIMESTAMP],"
            + "[TIMESTAMP,DATE],[TIMESTAMP,TIME],[TIMESTAMP,TIMESTAMP],[DATE,STRING],[TIME,STRING],[TIMESTAMP,STRING],[STRING,DATE],[STRING,TIME],[STRING,TIMESTAMP]},"
            + " but got [INTEGER,STRING]");
  }

  @Test
  public void testComparisonWithDifferentType() {
    getRelNode("source=EMP | where EMPNO > 6 | fields ENAME");
    getRelNode("source=EMP | where ENAME <= 'Jack' | fields ENAME");
    String ppl = "source=EMP | where ENAME < 6 | fields ENAME";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t, "LESS function expects {[COMPARABLE_TYPE,COMPARABLE_TYPE]}, but got [STRING,INTEGER]");
  }

  @Test
  public void testCoalesceWithDifferentType() {
    String ppl =
        "source=EMP | eval coalesce_name = coalesce(EMPNO, 'Jack', ENAME) | fields"
            + " coalesce_name";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t, "COALESCE function expects {[COMPARABLE_TYPE...]}, but got [SHORT,STRING,STRING]");
  }

  @Test
  public void testSubstringWithWrongType() {
    getRelNode("source=EMP | eval sub_name = substring(ENAME, 1, 3) | fields sub_name");
    getRelNode("source=EMP | eval sub_name = substring(ENAME, 1) | fields sub_name");
    String ppl = "source=EMP | eval sub_name = substring(ENAME, 1, '3') | fields sub_name";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t,
        "SUBSTRING function expects {[STRING,INTEGER],[STRING,INTEGER,INTEGER]}, but got"
            + " [STRING,INTEGER,STRING]");
  }

  @Test
  public void testIfWithWrongType() {
    getRelNode("source=EMP | eval if_name = if(EMPNO > 6, 'Jack', ENAME) | fields if_name");
    getRelNode("source=EMP | eval if_name = if(EMPNO > 6, EMPNO, DEPTNO) | fields if_name");
    String pplWrongCondition = "source=EMP | eval if_name = if(EMPNO, 1, DEPTNO) | fields if_name";
    Throwable t1 =
        Assert.assertThrows(
            ExpressionEvaluationException.class, () -> getRelNode(pplWrongCondition));
    verifyErrorMessageContains(
        t1, "IF function expects {[BOOLEAN,ANY,ANY]}, but got [SHORT,INTEGER,BYTE]");
    String pplIncompatibleType =
        "source=EMP | eval if_name = if(EMPNO > 6, 'Jack', 1) | fields if_name";
    Throwable t2 =
        Assert.assertThrows(IllegalArgumentException.class, () -> getRelNode(pplIncompatibleType));
    verifyErrorMessageContains(
        t2,
        "Cannot resolve function: IF, arguments: [BOOLEAN, VARCHAR, INTEGER], caused by: Can't find"
            + " leastRestrictive type for [VARCHAR, INTEGER]");
  }

  @Test
  public void testTimestampWithWrongArg() {
    String ppl =
        "source=EMP | eval timestamp = timestamp('2020-08-26 13:49:00', 2009) | fields timestamp |"
            + " head 1";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t,
        "TIMESTAMP function expects"
            + " {[STRING],[DATE],[TIME],[TIMESTAMP],[STRING,STRING],[DATE,DATE],[DATE,TIME],[DATE,TIMESTAMP],"
            + "[TIME,DATE],[TIME,TIME],[TIME,TIMESTAMP],[TIMESTAMP,DATE],[TIMESTAMP,TIME],[TIMESTAMP,TIMESTAMP]"
            + ",[STRING,DATE],[STRING,TIME],[STRING,TIMESTAMP],[DATE,STRING],[TIME,STRING],[TIMESTAMP,STRING]},"
            + " but got [STRING,INTEGER]");
  }

  @Test
  public void testCurDateWithArg() {
    Throwable t =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | eval curdate = CURDATE(1) | fields curdate | head 1"));
    verifyErrorMessageContains(t, "CURDATE function expects {[]}, but got [INTEGER]");
  }

  // Test directly registered UDF: register(funcname, FuncImp)
  @Test
  public void testLtrimWrongArg() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | where ltrim(EMPNO, DEPTNO) = 'Jim' | fields name, age"));
    verifyErrorMessageContains(e, "LTRIM function expects {[STRING]}, but got [SHORT,BYTE]");
  }

  // Test udf registered via sql library operator: registerOperator(REVERSE,
  // SqlLibraryOperators.REVERSE);
  @Test
  public void testReverseWrongArgShouldThrow() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | where reverse(EMPNO) = '3202' | fields year"));
    verifyErrorMessageContains(e, "REVERSE function expects {[STRING]}, but got [SHORT]");
  }

  // test type checking on UDF with direct registration: register(funcname, FuncImp)
  @Test
  public void testStrCmpWrongArgShouldThrow() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | where strcmp(10, 'Jane') = 0 | fields name, age"));
    verifyErrorMessageContains(
        e, "STRCMP function expects {[STRING,STRING]}, but got [INTEGER,STRING]");
  }

  // Test registered Sql Std Operator: registerOperator(funcName, SqlStdOperatorTable.OPERATOR)
  @Test
  public void testLowerWrongArgShouldThrow() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | where lower(EMPNO) = 'hello' | fields name, age"));
    verifyErrorMessageContains(e, "LOWER function expects {[STRING]}, but got [SHORT]");
  }

  @Test
  public void testSha2WrongArgShouldThrow() {
    Throwable e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () ->
                getRelNode(
                    "source=EMP | head 1 | eval sha256 = SHA2('hello', '256') | fields sha256"));
    verifyErrorMessageContains(
        e, "SHA2 function expects {[STRING,INTEGER]}, but got [STRING,STRING]");
  }

  // Test type checking on udf with direct registration: register(SQRT, funcImp)
  @Test
  public void testSqrtWithWrongArg() {
    Exception nanException =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () ->
                getRelNode(
                    "source=EMP | head 1 | eval sqrt_name = sqrt(ENAME) | fields sqrt_name"));
    verifyErrorMessageContains(
        nanException, "SQRT function expects {[INTEGER],[DOUBLE]}, but got [STRING]");
  }

  // Test UDF registered with PPL builtin operators: registerOperator(MOD, PPLBuiltinOperators.MOD);
  @Test
  public void testModWithWrongArg() {
    Exception wrongArgException =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | eval z = mod(0.5, 1, 2) | fields z"));
    verifyErrorMessageContains(
        wrongArgException,
        "MOD function expects"
            + " {[INTEGER,INTEGER],[INTEGER,DOUBLE],[DOUBLE,INTEGER],[DOUBLE,DOUBLE]}, but got"
            + " [DOUBLE,INTEGER,INTEGER]");
  }

  // Test UDF registered with sql std operators: registerOperator(PI, SqlStdOperatorTable.PI)
  @Test
  public void testPiWithArg() {
    Exception wrongArgException =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | eval pi = pi(1) | fields pi"));
    verifyErrorMessageContains(wrongArgException, "PI function expects {[]}, but got [INTEGER]");
  }

  // Test UDF registered with sql library operators: registerOperator(LOG2,
  // SqlLibraryOperators.LOG2)
  @Test
  public void testLog2WithWrongArgShouldThrow() {
    Exception wrongArgException =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | eval log2 = log2(ENAME, JOB) | fields log2"));
    verifyErrorMessageContains(
        wrongArgException, "LOG2 function expects {[INTEGER],[DOUBLE]}, but got [STRING,STRING]");
  }
}
