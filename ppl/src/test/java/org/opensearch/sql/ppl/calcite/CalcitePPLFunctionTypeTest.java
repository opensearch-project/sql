/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertNotNull;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Assert;
import org.junit.Test;

public class CalcitePPLFunctionTypeTest extends CalcitePPLAbstractTest {

  public CalcitePPLFunctionTypeTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testLowerWithIntegerType() {
    // Lower with IntegerType no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | eval lower_name = lower(EMPNO) | fields lower_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(lower_name=[LOWER($0)])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testTimeDiffWithUdtInputType() {
    // TimeDiff with UdtInputType no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | eval time_diff = timediff(12, '2009-12-10') | fields time_diff";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(time_diff=[TIME_DIFF(12, '2009-12-10':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testComparisonWithDifferentType() {
    getRelNode("source=EMP | where EMPNO > 6 | fields ENAME");
    getRelNode("source=EMP | where ENAME <= 'Jack' | fields ENAME");
    // LogicalFilter(condition=[<(SAFE_CAST($1), 6.0E0)])
    getRelNode("source=EMP | where ENAME < 6 | fields ENAME");
  }

  /**
   * Test that safe numeric widening (e.g., SMALLINT → INTEGER) uses regular CAST instead of
   * SAFE_CAST. This avoids generating scripts in OpenSearch queries, improving performance.
   *
   * <p>EMPNO is SMALLINT in the SCOTT schema, and literal 6 is INTEGER. The comparison requires
   * widening EMPNO to INTEGER. Since SMALLINT → INTEGER is a safe widening (no data loss), regular
   * CAST is used instead of SAFE_CAST.
   *
   * <p>With regular CAST, Calcite can optimize it away entirely because numeric comparisons between
   * compatible integer types are semantically equivalent. With SAFE_CAST, the cast would be
   * preserved because SAFE_CAST has different semantics (returns NULL on failure).
   */
  @Test
  public void testSafeNumericWideningUsesCastInsteadOfSafeCast() {
    // EMPNO is SMALLINT, 6 is INTEGER
    // Safe widening SMALLINT → INTEGER uses CAST, which Calcite optimizes away
    // The result is a direct comparison >($0, 6) without any cast
    RelNode root = getRelNode("source=EMP | where EMPNO > 6 | fields ENAME");
    verifyLogical(
        root,
        "LogicalProject(ENAME=[$1])\n"
            + "  LogicalFilter(condition=[>($0, 6)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
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
    // Substring with wrong type no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | eval sub_name = substring(ENAME, 1, '3') | fields sub_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(sub_name=[SUBSTRING($1, 1, '3')])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testIfWithWrongType() {
    // If with wrong type no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | eval if_name = if(EMPNO, 1, DEPTNO) | fields if_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(if_name=[CASE($0, 1, $7)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testTimestampWithWrongArg() {
    // Timestamp with wrong argument no longer throws exception, Calcite handles type coercion
    String ppl =
        "source=EMP | eval timestamp = timestamp('2020-08-26 13:49:00', 2009) | fields timestamp |"
            + " head 1";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(timestamp=[TIMESTAMP('2020-08-26 13:49:00':VARCHAR, 2009)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testCurDateWithArg() {
    // CurDate with Arg no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | eval curdate = CURDATE(1) | fields curdate | head 1";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(curdate=[CURRENT_DATE(1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
  }

  // Test directly registered UDF: register(funcname, FuncImp)
  // Test directly registered UDF: register(funcname, FuncImp)
  @Test
  public void testLtrimWrongArg() {
    String ppl = "source=EMP | where ltrim(EMPNO, DEPTNO) = 'Jim' | fields name, age";
    Exception e = Assert.assertThrows(IllegalArgumentException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "This function requires exactly 1 arguments");
  }

  // Test udf registered via sql library operator: registerOperator(REVERSE,
  // SqlLibraryOperators.REVERSE);
  // Test udf registered via sql library operator: registerOperator(REVERSE,
  // SqlLibraryOperators.REVERSE);
  @Test
  public void testReverseWrongArgShouldThrow() {
    String ppl = "source=EMP | where reverse(EMPNO) = '3202' | fields year";
    Throwable e = Assert.assertThrows(AssertionError.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "Was not expecting value 'SMALLINT'");
  }

  // test type checking on UDF with direct registration: register(funcname, FuncImp)
  // test type checking on UDF with direct registration: register(funcname, FuncImp)
  @Test
  public void testStrCmpWrongArgShouldThrow() {
    String ppl = "source=EMP | where strcmp(10, 'Jane') = 0 | fields name, age";
    Exception e = Assert.assertThrows(IllegalArgumentException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "Field [name] not found");
  }

  // Test registered Sql Std Operator: registerOperator(funcName, SqlStdOperatorTable.OPERATOR)
  // Test registered Sql Std Operator: registerOperator(funcName, SqlStdOperatorTable.OPERATOR)
  @Test
  public void testLowerWrongArgShouldThrow() {
    String ppl = "source=EMP | where lower(EMPNO) = 'hello' | fields name, age";
    Exception e = Assert.assertThrows(IllegalArgumentException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(e, "Field [name] not found");
  }

  @Test
  public void testSha2WrongArgShouldThrow() {
    // Sha2WrongArg should throw no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | head 1 | eval sha256 = SHA2('hello', '256') | fields sha256";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(sha256=[SHA2('hello':VARCHAR, '256':VARCHAR)])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
  }

  // Test type checking on udf with direct registration: register(SQRT, funcImp)
  @Test
  public void testSqrtWithWrongArg() {
    // Sqrt with wrong argument no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | head 1 | eval sqrt_name = sqrt(HIREDATE) | fields sqrt_name";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(sqrt_name=[POWER($4, 0.5E0:DOUBLE)])\n"
            + "  LogicalSort(fetch=[1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
  }

  // Test UDF registered with PPL builtin operators: registerOperator(MOD, PPLBuiltinOperators.MOD);
  @Test
  public void testModWithWrongArg() {
    // Mod with wrong argument no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | eval z = mod(0.5, 1, 2) | fields z";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(z=[MOD(0.5:DECIMAL(2, 1), 1, 2)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  // Test UDF registered with sql std operators: registerOperator(PI, SqlStdOperatorTable.PI)
  @Test
  public void testPiWithArg() {
    // Pi with Arg no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | eval pi = pi(1) | fields pi";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root, "LogicalProject(pi=[PI(1)])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  // Test UDF registered with sql library operators: registerOperator(LOG2,
  // SqlLibraryOperators.LOG2)
  @Test
  public void testLog2WithWrongArgShouldThrow() {
    // Log2 with wrong argument should throw no longer throws exception, Calcite handles type
    // coercion
    String ppl = "source=EMP | eval log2 = log2(ENAME, JOB) | fields log2";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(log2=[LOG2($1, $2)])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n");
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
    // First argument should be numeric/timestamp, but Calcite handles type coercion
    String ppl = "source=EMP | eval formatted = strftime(EMPNO > 5, '%Y-%m-%d') | fields formatted";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(formatted=[STRFTIME(>($0, 5), '%Y-%m-%d':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testStrftimeAcceptsDateInput() {
    // DATE values are automatically converted to TIMESTAMP by Calcite
    // This test verifies that DATE inputs work via auto-conversion
    String ppl =
        "source=EMP | eval formatted = strftime(date('2020-09-16'), '%Y-%m-%d') | fields formatted";
    RelNode relNode = getRelNode(ppl);
    assertNotNull(relNode);
    verifyLogical(
        relNode,
        "LogicalProject(formatted=[STRFTIME(DATE('2020-09-16':VARCHAR), '%Y-%m-%d':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
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
    // Second argument should be string, but Calcite handles type coercion
    String ppl = "source=EMP | eval formatted = strftime(1521467703, 123) | fields formatted";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalProject(formatted=[STRFTIME(1521467703, 123)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  @Test
  public void testStrftimeWithWrongNumberOfArgs() {
    // strftime now accepts variable arguments, so these no longer throw exceptions
    String ppl1 = "source=EMP | eval formatted = strftime(1521467703) | fields formatted";
    RelNode root1 = getRelNode(ppl1);
    verifyLogical(
        root1,
        "LogicalProject(formatted=[STRFTIME(1521467703)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");

    String ppl2 =
        "source=EMP | eval formatted = strftime(1521467703, '%Y', 'extra') | fields formatted";
    RelNode root2 = getRelNode(ppl2);
    verifyLogical(
        root2,
        "LogicalProject(formatted=[STRFTIME(1521467703, '%Y':VARCHAR, 'extra':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n");
  }

  // Test VALUES function with array expression (which is not a supported scalar type)
  @Test
  public void testValuesFunctionWithArrayArgType() {
    // ValuesFunction with ArrayArgType no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | stats values(array(ENAME, JOB)) as unique_values";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalAggregate(group=[{}], unique_values=[VALUES($0)])\n"
            + "  LogicalProject($f2=[array($1, $2)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
  }

  // mvjoin should reject non-string single values
  @Test
  public void testMvjoinRejectsNonStringValues() {
    // Mvjoin rejects non-stringValues no longer throws exception, Calcite handles type coercion
    String ppl = "source=EMP | eval result = mvjoin(42, ',') | fields result | head 1";
    RelNode root = getRelNode(ppl);
    verifyLogical(
        root,
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(result=[ARRAY_JOIN(42, ',')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n");
  }
}
