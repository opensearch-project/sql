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
        t, "TIMEDIFF function expects {[TIME,TIME]}, but got [INTEGER,STRING]");
  }

  @Test
  public void testComparisonWithDifferentType() {
    getRelNode("source=EMP | where EMPNO > 6 | fields ENAME");
    getRelNode("source=EMP | where ENAME <= 'Jack' | fields ENAME");
    String ppl = "source=EMP | where ENAME < 6 | fields ENAME";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t,
        // Temporary fix for the error message as LESS function has two variants. Will remove
        // [IP,IP] when merging the two variants.
        "LESS function expects {[IP,IP],[COMPARABLE_TYPE,COMPARABLE_TYPE]},"
            + " but got [STRING,INTEGER]");
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
        Assert.assertThrows(
            ExpressionEvaluationException.class, () -> getRelNode(pplIncompatibleType));
    verifyErrorMessageContains(
        t2,
        "Cannot resolve function: IF, arguments: [BOOLEAN,STRING,INTEGER], caused by: Can't find"
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
        "TIMESTAMP function expects {"
            + "[STRING],[TIMESTAMP],[DATE],[TIME],[STRING,STRING],[TIMESTAMP,TIMESTAMP],[TIMESTAMP,DATE],"
            + "[TIMESTAMP,TIME],[DATE,TIMESTAMP],[DATE,DATE],[DATE,TIME],[TIME,TIMESTAMP],[TIME,DATE],"
            + "[TIME,TIME],[STRING,TIMESTAMP],[STRING,DATE],[STRING,TIME],[TIMESTAMP,STRING],[DATE,STRING],[TIME,STRING]},"
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

  @Test
  public void testAvgWithWrongArgType() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats avg(ENAME) as avg_name"));
    verifyErrorMessageContains(
        e, "Aggregation function AVG expects field type {[INTEGER],[DOUBLE]}, but got [STRING]");
  }

  @Test
  public void testVarsampWithWrongArgType() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats var_samp(ENAME) as varsamp_name"));
    verifyErrorMessageContains(
        e,
        "Aggregation function VARSAMP expects field type {[INTEGER],[DOUBLE]}, but got [STRING]");
  }

  @Test
  public void testVarpopWithWrongArgType() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats var_pop(ENAME) as varpop_name"));
    verifyErrorMessageContains(
        e, "Aggregation function VARPOP expects field type {[INTEGER],[DOUBLE]}, but got [STRING]");
  }

  @Test
  public void testStddevSampWithWrongArgType() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats stddev_samp(ENAME) as stddev_name"));
    verifyErrorMessageContains(
        e,
        "Aggregation function STDDEV_SAMP expects field type {[INTEGER],[DOUBLE]}, but got"
            + " [STRING]");
  }

  @Test
  public void testStddevPopWithWrongArgType() {
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats stddev_pop(ENAME) as stddev_name"));
    verifyErrorMessageContains(
        e,
        "Aggregation function STDDEV_POP expects field type {[INTEGER],[DOUBLE]}, but got"
            + " [STRING]");
  }

  @Test
  public void testPercentileApproxWithWrongArgType() {
    // First argument should be numeric
    Exception e1 =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats percentile_approx(ENAME, 50) as percentile"));
    verifyErrorMessageContains(
        e1,
        "Aggregation function PERCENTILE_APPROX expects field type and additional arguments"
            + " {[INTEGER,INTEGER],[INTEGER,DOUBLE],[DOUBLE,INTEGER],[DOUBLE,DOUBLE],[INTEGER,INTEGER,INTEGER],[INTEGER,INTEGER,DOUBLE],[INTEGER,DOUBLE,INTEGER],[INTEGER,DOUBLE,DOUBLE],[DOUBLE,INTEGER,INTEGER],[DOUBLE,INTEGER,DOUBLE],[DOUBLE,DOUBLE,INTEGER],[DOUBLE,DOUBLE,DOUBLE]},"
            + " but got [STRING,INTEGER]");
  }

  @Test
  public void testListFunctionWithScalarTypes() {
    // Test that LIST function works with various scalar types available in SCOTT schema
    getRelNode("source=EMP | stats list(EMPNO) as emp_numbers");
    getRelNode("source=EMP | stats list(ENAME) as names");
    getRelNode("source=EMP | stats list(HIREDATE) as hire_dates");
    getRelNode("source=EMP | stats list(DEPTNO) as dept_numbers");
  }

  @Test
  public void testValuesFunctionWithScalarTypes() {
    // Test that VALUES function works with various scalar types available in SCOTT schema
    getRelNode("source=EMP | stats values(EMPNO) as unique_emp_numbers");
    getRelNode("source=EMP | stats values(ENAME) as unique_names");
    getRelNode("source=EMP | stats values(HIREDATE) as unique_hire_dates");
    getRelNode("source=EMP | stats values(DEPTNO) as unique_dept_numbers");
  }

  @Test
  public void testListFunctionWithWrongArgType() {
    // Test LIST function with array expression (which is not a supported scalar type)
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats list(array(ENAME, JOB)) as name_list"));
    verifyErrorMessageContains(
        e,
        "Aggregation function LIST expects field type"
            + " {[BOOLEAN],[BYTE],[SHORT],[INTEGER],[LONG],[FLOAT],[DOUBLE],[STRING],[DATE],[TIME],[TIMESTAMP],[IP],[BINARY],[GEO_POINT]},"
            + " but got [ARRAY]");
  }

  @Test
  public void testValuesFunctionWithWrongArgType() {
    // Test VALUES function with array expression (which is not a supported scalar type)
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats values(array(ENAME, JOB)) as unique_names"));
    verifyErrorMessageContains(
        e,
        "Aggregation function VALUES expects field type"
            + " {[BOOLEAN],[BYTE],[SHORT],[INTEGER],[LONG],[FLOAT],[DOUBLE],[STRING],[DATE],[TIME],[TIMESTAMP],[IP],[BINARY],[GEO_POINT]},"
            + " but got [ARRAY]");
  }

  @Test
  public void testListFunctionWithMapExpression() {
    // Test LIST function with complex expression that creates a non-scalar type
    // Using array() function which should definitely return ARRAY type
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats list(array(ENAME, JOB)) as employee_arrays"));
    verifyErrorMessageContains(
        e,
        "Aggregation function LIST expects field type"
            + " {[BOOLEAN],[BYTE],[SHORT],[INTEGER],[LONG],[FLOAT],[DOUBLE],[STRING],[DATE],[TIME],[TIMESTAMP],[IP],[BINARY],[GEO_POINT]},"
            + " but got [ARRAY]");
  }

  @Test
  public void testValuesFunctionWithMapExpression() {
    // Test VALUES function with complex expression that creates a non-scalar type
    // Using array() function which should definitely return ARRAY type
    Exception e =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats values(array(ENAME, JOB)) as unique_arrays"));
    verifyErrorMessageContains(
        e,
        "Aggregation function VALUES expects field type"
            + " {[BOOLEAN],[BYTE],[SHORT],[INTEGER],[LONG],[FLOAT],[DOUBLE],[STRING],[DATE],[TIME],[TIMESTAMP],[IP],[BINARY],[GEO_POINT]},"
            + " but got [ARRAY]");
  }

  @Test
  public void testListFunctionAcceptsAllScalarTypes() {
    // Test that LIST function accepts all supported scalar types
    getRelNode("source=EMP | stats list(EMPNO) as numeric_list"); // NUMERIC
    getRelNode("source=EMP | stats list(ENAME) as string_list"); // CHARACTER
    getRelNode("source=EMP | stats list(HIREDATE) as date_list"); // DATE/TIMESTAMP
    getRelNode(
        "source=EMP | eval is_high = EMPNO > 100 | stats list(is_high) as boolean_list"); // BOOLEAN
  }

  @Test
  public void testValuesFunctionAcceptsAllScalarTypes() {
    // Test that VALUES function accepts all supported scalar types
    getRelNode("source=EMP | stats values(EMPNO) as unique_numbers"); // NUMERIC
    getRelNode("source=EMP | stats values(ENAME) as unique_strings"); // CHARACTER
    getRelNode("source=EMP | stats values(HIREDATE) as unique_dates"); // DATE/TIMESTAMP
    getRelNode(
        "source=EMP | eval is_high = EMPNO > 100 | stats values(is_high) as"
            + " unique_booleans"); // BOOLEAN
  }

  @Test
  public void testListAndValuesFunctionsErrorMessageFormat() {
    // Test that error messages follow the expected aggregate function format
    Exception listException =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats list(array(ENAME)) as names"));

    Exception valuesException =
        Assert.assertThrows(
            ExpressionEvaluationException.class,
            () -> getRelNode("source=EMP | stats values(array(ENAME)) as unique_names"));

    // Verify both functions use the standard "Aggregation function" error format
    verifyErrorMessageContains(listException, "Aggregation function LIST expects field type");
    verifyErrorMessageContains(valuesException, "Aggregation function VALUES expects field type");

    // Verify both show the same allowed types
    verifyErrorMessageContains(
        listException,
        "[BOOLEAN],[BYTE],[SHORT],[INTEGER],[LONG],[FLOAT],[DOUBLE],[STRING],[DATE],[TIME],[TIMESTAMP],[IP],[BINARY],[GEO_POINT]");
    verifyErrorMessageContains(
        valuesException,
        "[BOOLEAN],[BYTE],[SHORT],[INTEGER],[LONG],[FLOAT],[DOUBLE],[STRING],[DATE],[TIME],[TIMESTAMP],[IP],[BINARY],[GEO_POINT]");
  }
}
