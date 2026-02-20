/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.RelNode;
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
    // LogicalFilter(condition=[<(SAFE_CAST($1), 6.0E0)])
    getRelNode("source=EMP | where ENAME < 6 | fields ENAME");
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
        "source=EMP | head 1 | eval sqrt_name = sqrt(HIREDATE) | fields sqrt_name",
        "SQRT function expects {[INTEGER]|[DOUBLE]}, but got [DATE]");
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
  public void testStrftimeWithCorrectTypes() {
    // Test with integer timestamp and string format
    getRelNode("source=EMP | eval formatted = strftime(1521467703, '%Y-%m-%d') | fields formatted");
    // Test with double timestamp and string format
    getRelNode(
        "source=EMP | eval formatted = strftime(1521467703.0, '%Y-%m-%d %H:%M:%S') | fields"
            + " formatted");
    // Test with expression that returns numeric type
    getRelNode(
        "source=EMP | eval formatted = strftime(EMPNO * 1000000, '%F %T') | fields formatted");
    // Test with timestamp from now()
    getRelNode("source=EMP | eval formatted = strftime(now(), '%Y-%m-%d') | fields formatted");
    // Test with timestamp from from_unixtime()
    getRelNode(
        "source=EMP | eval formatted = strftime(from_unixtime(1521467703), '%Y-%m-%d') | fields"
            + " formatted");
  }

  @Test
  public void testStrftimeWithWrongFirstArgType() {
    // First argument should be numeric/timestamp, not boolean
    String ppl = "source=EMP | eval formatted = strftime(EMPNO > 5, '%Y-%m-%d') | fields formatted";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t,
        "STRFTIME function expects {[INTEGER,STRING]|[DOUBLE,STRING]|[TIMESTAMP,STRING]}, but got"
            + " [BOOLEAN,STRING]");
  }

  @Test
  public void testStrftimeAcceptsDateInput() {
    // DATE values are automatically converted to TIMESTAMP by Calcite
    // This test verifies that DATE inputs work via auto-conversion
    String ppl =
        "source=EMP | eval formatted = strftime(date('2020-09-16'), '%Y-%m-%d') | fields formatted";
    RelNode relNode = getRelNode(ppl);
    assertNotNull(relNode);
    // The plan should show TIMESTAMP(DATE(...)) indicating auto-conversion
    String planString = relNode.explain();
    assertTrue(planString.contains("STRFTIME") && planString.contains("TIMESTAMP"));
  }

  @Test
  public void testStrftimeWithDateReturningFunctions() {
    // Test strftime with various functions that return DATE/TIMESTAMP types

    // Test with NOW() function
    String ppl1 =
        "source=EMP | eval formatted = strftime(now(), '%Y-%m-%d %H:%M:%S') | fields formatted";
    RelNode relNode1 = getRelNode(ppl1);
    assertNotNull(relNode1);

    // Test with TIMESTAMP function
    String ppl2 =
        "source=EMP | eval formatted = strftime(timestamp('2020-09-16 10:30:45'), '%Y-%m-%d"
            + " %H:%M:%S') | fields formatted";
    RelNode relNode2 = getRelNode(ppl2);
    assertNotNull(relNode2);

    // Test with FROM_UNIXTIME (returns TIMESTAMP)
    String ppl3 =
        "source=EMP | eval formatted = strftime(from_unixtime(1521467703), '%Y-%m-%d %H:%M:%S') |"
            + " fields formatted";
    RelNode relNode3 = getRelNode(ppl3);
    assertNotNull(relNode3);

    // Test with chained date functions
    String ppl4 =
        "source=EMP | eval ts = timestamp('2020-09-16 10:30:45') | eval formatted = strftime(ts,"
            + " '%F %T') | fields formatted";
    RelNode relNode4 = getRelNode(ppl4);
    assertNotNull(relNode4);
  }

  @Test
  public void testStrftimeWithWrongSecondArgType() {
    // Second argument should be string, not numeric
    String ppl = "source=EMP | eval formatted = strftime(1521467703, 123) | fields formatted";
    Throwable t = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(
        t,
        "STRFTIME function expects {[INTEGER,STRING]|[DOUBLE,STRING]|[TIMESTAMP,STRING]}, but got"
            + " [INTEGER,INTEGER]");
  }

  @Test
  public void testStrftimeWithWrongNumberOfArgs() {
    // strftime requires exactly 2 arguments
    String ppl1 = "source=EMP | eval formatted = strftime(1521467703) | fields formatted";
    Throwable t1 = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl1));
    verifyErrorMessageContains(
        t1,
        "STRFTIME function expects {[INTEGER,STRING]|[DOUBLE,STRING]|[TIMESTAMP,STRING]}, but got"
            + " [INTEGER]");

    String ppl2 =
        "source=EMP | eval formatted = strftime(1521467703, '%Y', 'extra') | fields formatted";
    Throwable t2 = Assert.assertThrows(ExpressionEvaluationException.class, () -> getRelNode(ppl2));
    verifyErrorMessageContains(
        t2,
        "STRFTIME function expects {[INTEGER,STRING]|[DOUBLE,STRING]|[TIMESTAMP,STRING]}, but got"
            + " [INTEGER,STRING,STRING]");
  }

  // Test VALUES function with array expression (which is not a supported scalar type)
  @Test
  public void testValuesFunctionWithArrayArgType() {
    verifyQueryThrowsException(
        "source=EMP | stats values(array(ENAME, JOB)) as unique_values",
        "Aggregation function VALUES expects field type"
            + " {[BYTE]|[SHORT]|[INTEGER]|[LONG]|[FLOAT]|[DOUBLE]|[STRING]|[BOOLEAN]|[DATE]|[TIME]|[TIMESTAMP]|[IP]|[BINARY]|[BYTE,INTEGER]"
            + "|[SHORT,INTEGER]|[INTEGER,INTEGER]|[LONG,INTEGER]|[FLOAT,INTEGER]|[DOUBLE,INTEGER]|[STRING,INTEGER]|[BOOLEAN,INTEGER]|[DATE,INTEGER]|[TIME,INTEGER]|[TIMESTAMP,INTEGER]|[IP,INTEGER]|[BINARY,INTEGER]},"
            + " but got [ARRAY]");
  }

  // mvjoin should reject non-string single values
  @Test
  public void testMvjoinRejectsNonStringValues() {
    verifyQueryThrowsException(
        "source=EMP | eval result = mvjoin(42, ',') | fields result | head 1",
        "MVJOIN function expects {[ARRAY,STRING]|[ARRAY,STRING,STRING]}, but got [INTEGER,STRING]");
  }
}
