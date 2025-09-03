/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.test.CalciteAssert;
import org.junit.Assert;
import org.junit.Test;

public class CalcitePPLFunctionTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLFunctionTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testLowerWithIntegerType() {
    verifyQueryThrowsException(
        "source=EMP | eval lower_name = lower(EMPNO) | fields lower_name",
        "LOWER function expects {[STRING]}, but got [SHORT]");
  }

  @Test
  public void testTimeDiffWithUdtInputType() {
    String strPpl =
        "source=EMP | eval time_diff = timediff('12:00:00', '12:00:06') | fields time_diff";
    String timePpl =
        "source=EMP | eval time_diff = timediff(time('13:00:00'), time('12:00:06')) | fields"
            + " time_diff";
    getRelNode(strPpl);
    getRelNode(timePpl);
    verifyQueryThrowsException(
        "source=EMP | eval time_diff = timediff(12, '2009-12-10') | fields time_diff",
        "TIMEDIFF function expects {[TIME,TIME]}, but got [INTEGER,STRING]");
  }

  @Test
  public void testComparisonWithDifferentType() {
    getRelNode("source=EMP | where EMPNO > 6 | fields ENAME");
    getRelNode("source=EMP | where ENAME <= 'Jack' | fields ENAME");
    verifyQueryThrowsException(
        "source=EMP | where ENAME < 6 | fields ENAME",
        // Temporary fix for the error message as LESS function has two variants. Will remove
        // [IP,IP] when merging the two variants.
        "LESS function expects {[IP,IP],[COMPARABLE_TYPE,COMPARABLE_TYPE]}, but got"
            + " [STRING,INTEGER]");
  }

  @Test
  public void testCoalesceWithSameType() {
    String ppl = "source=EMP | eval coalesce_name = coalesce(ENAME, 'Jack') | fields coalesce_name";
    Assert.assertNotNull(getRelNode(ppl));
  }

  @Test
  public void testCoalesceWithDifferentType() {
    String ppl =
        "source=EMP | eval coalesce_name = coalesce(EMPNO, 'Jack', ENAME) | fields"
            + " coalesce_name";
    Assert.assertNotNull(getRelNode(ppl));
  }

  @Test
  public void testSubstringWithWrongType() {
    getRelNode("source=EMP | eval sub_name = substring(ENAME, 1, 3) | fields sub_name");
    getRelNode("source=EMP | eval sub_name = substring(ENAME, 1) | fields sub_name");
    verifyQueryThrowsException(
        "source=EMP | eval sub_name = substring(ENAME, 1, '3') | fields sub_name",
        "SUBSTRING function expects {[STRING,INTEGER]|[STRING,INTEGER,INTEGER]}, but got"
            + " [STRING,INTEGER,STRING]");
  }

  @Test
  public void testIfWithWrongType() {
    getRelNode("source=EMP | eval if_name = if(EMPNO > 6, 'Jack', ENAME) | fields if_name");
    getRelNode("source=EMP | eval if_name = if(EMPNO > 6, EMPNO, DEPTNO) | fields if_name");
    verifyQueryThrowsException(
        "source=EMP | eval if_name = if(EMPNO, 1, DEPTNO) | fields if_name",
        "IF function expects {[BOOLEAN,ANY,ANY]}, but got [SHORT,INTEGER,BYTE]");
    verifyQueryThrowsException(
        "source=EMP | eval if_name = if(EMPNO > 6, 'Jack', 1) | fields if_name",
        "Cannot resolve function: IF, arguments: [BOOLEAN,STRING,INTEGER], caused by: Can't find"
            + " leastRestrictive type for [VARCHAR, INTEGER]");
  }

  @Test
  public void testTimestampWithWrongArg() {
    verifyQueryThrowsException(
        "source=EMP | eval timestamp = timestamp('2020-08-26 13:49:00', 2009) | fields timestamp |"
            + " head 1",
        "TIMESTAMP function expects"
            + " {[STRING]|[TIMESTAMP]|[DATE]|[TIME]|[STRING,STRING]|[TIMESTAMP,TIMESTAMP]|[TIMESTAMP,DATE]|[TIMESTAMP,TIME]|[DATE,TIMESTAMP]|[DATE,DATE]|[DATE,TIME]|[TIME,TIMESTAMP]|[TIME,DATE]|[TIME,TIME]|[STRING,TIMESTAMP]|[STRING,DATE]|[STRING,TIME]|[TIMESTAMP,STRING]|[DATE,STRING]|[TIME,STRING]},"
            + " but got [STRING,INTEGER]");
  }

  @Test
  public void testCurDateWithArg() {
    verifyQueryThrowsException(
        "source=EMP | eval curdate = CURDATE(1) | fields curdate | head 1",
        "CURDATE function expects {[]}, but got [INTEGER]");
  }

  // Test directly registered UDF: register(funcname, FuncImp)
  @Test
  public void testLtrimWrongArg() {
    verifyQueryThrowsException(
        "source=EMP | where ltrim(EMPNO, DEPTNO) = 'Jim' | fields name, age",
        "LTRIM function expects {[STRING]}, but got [SHORT,BYTE]");
  }

  // Test udf registered via sql library operator: registerOperator(REVERSE,
  // SqlLibraryOperators.REVERSE);
  @Test
  public void testReverseWrongArgShouldThrow() {
    verifyQueryThrowsException(
        "source=EMP | where reverse(EMPNO) = '3202' | fields year",
        "REVERSE function expects {[STRING]}, but got [SHORT]");
  }

  // test type checking on UDF with direct registration: register(funcname, FuncImp)
  @Test
  public void testStrCmpWrongArgShouldThrow() {
    verifyQueryThrowsException(
        "source=EMP | where strcmp(10, 'Jane') = 0 | fields name, age",
        "STRCMP function expects {[STRING,STRING]}, but got [INTEGER,STRING]");
  }

  // Test registered Sql Std Operator: registerOperator(funcName, SqlStdOperatorTable.OPERATOR)
  @Test
  public void testLowerWrongArgShouldThrow() {
    verifyQueryThrowsException(
        "source=EMP | where lower(EMPNO) = 'hello' | fields name, age",
        "LOWER function expects {[STRING]}, but got [SHORT]");
  }

  @Test
  public void testSha2WrongArgShouldThrow() {
    verifyQueryThrowsException(
        "source=EMP | head 1 | eval sha256 = SHA2('hello', '256') | fields sha256",
        "SHA2 function expects {[STRING,INTEGER]}, but got [STRING,STRING]");
  }

  // Test type checking on udf with direct registration: register(SQRT, funcImp)
  @Test
  public void testSqrtWithWrongArg() {
    verifyQueryThrowsException(
        "source=EMP | head 1 | eval sqrt_name = sqrt(ENAME) | fields sqrt_name",
        "SQRT function expects {[INTEGER]|[DOUBLE]}, but got [STRING]");
  }

  // Test UDF registered with PPL builtin operators: registerOperator(MOD, PPLBuiltinOperators.MOD);
  @Test
  public void testModWithWrongArg() {
    verifyQueryThrowsException(
        "source=EMP | eval z = mod(0.5, 1, 2) | fields z",
        "MOD function expects"
            + " {[INTEGER,INTEGER]|[INTEGER,DOUBLE]|[DOUBLE,INTEGER]|[DOUBLE,DOUBLE]}, but got"
            + " [DOUBLE,INTEGER,INTEGER]");
  }

  // Test UDF registered with sql std operators: registerOperator(PI, SqlStdOperatorTable.PI)
  @Test
  public void testPiWithArg() {
    verifyQueryThrowsException(
        "source=EMP | eval pi = pi(1) | fields pi", "PI function expects {[]}, but got [INTEGER]");
  }

  // Test UDF registered with sql library operators: registerOperator(LOG2,
  // SqlLibraryOperators.LOG2)
  @Test
  public void testLog2WithWrongArgShouldThrow() {
    verifyQueryThrowsException(
        "source=EMP | eval log2 = log2(ENAME, JOB) | fields log2",
        "LOG2 function expects {[INTEGER]|[DOUBLE]}, but got [STRING,STRING]");
  }

  @Test
  public void testMvjoinRejectsNonStringValues() {
    // mvjoin should reject non-string single values
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () ->
                getRelNode("source=EMP | eval result = mvjoin(42, ',') | fields result | head 1"));

    verifyErrorMessageContains(
        e, "MVJOIN function expects {[STRING,STRING],[ARRAY,STRING]}, but got [INTEGER,STRING]");
  }
}
