/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLEnhancedCoalesceTest extends CalcitePPLAbstractTest {

  public CalcitePPLEnhancedCoalesceTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testCoalesceBasic() {
    String ppl =
        "source=EMP | eval result = coalesce(COMM, SAL, 0) | fields EMPNO, COMM, SAL, result | head"
            + " 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[3])\n"
            + "  LogicalProject(EMPNO=[$0], COMM=[$6], SAL=[$5], result=[COALESCE($6, $5, 0)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `COMM`, `SAL`, COALESCE(`COMM`, `SAL`, 0) `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithMixedTypes() {
    String ppl =
        "source=EMP | eval result = coalesce(COMM, EMPNO, 'fallback') | fields EMPNO, COMM, result"
            + " | head 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[3])\n"
            + "  LogicalProject(EMPNO=[$0], COMM=[$6], result=[COALESCE($6, $0,"
            + " 'fallback':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `COMM`, COALESCE(`COMM`, `EMPNO`, 'fallback') `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithLiterals() {
    String ppl =
        "source=EMP | eval result = coalesce(COMM, 123, 'unknown') | fields EMPNO, result | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], result=[COALESCE($6, 123, 'unknown':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(`COMM`, 123, 'unknown') `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceInWhere() {
    String ppl = "source=EMP | where coalesce(ENAME, 'UNKNOWN') = 'SMITH' | fields EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalFilter(condition=[=(COALESCE($1, 'UNKNOWN':VARCHAR), 'SMITH')])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE COALESCE(`ENAME`, 'UNKNOWN') = 'SMITH'";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithMultipleFields() {
    String ppl =
        "source=EMP | eval result = coalesce(COMM, SAL, MGR, EMPNO) | fields EMPNO, COMM, SAL, MGR,"
            + " result | head 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[2])\n"
            + "  LogicalProject(EMPNO=[$0], COMM=[$6], SAL=[$5], MGR=[$3], result=[COALESCE($6, $5,"
            + " $3, $0)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `COMM`, `SAL`, `MGR`, COALESCE(`COMM`, `SAL`, `MGR`, `EMPNO`) `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceNested() {
    String ppl =
        "source=EMP | eval result1 = coalesce(COMM, 0), result2 = coalesce(result1, SAL) | fields"
            + " EMPNO, COMM, SAL, result1, result2 | head 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[2])\n"
            + "  LogicalProject(EMPNO=[$0], COMM=[$6], SAL=[$5], result1=[COALESCE($6, 0)],"
            + " result2=[COALESCE(COALESCE($6, 0), $5)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `COMM`, `SAL`, COALESCE(`COMM`, 0) `result1`, COALESCE(COALESCE(`COMM`,"
            + " 0), `SAL`) `result2`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithNonExistentField() {
    String ppl =
        "source=EMP | eval result = coalesce(nonexistent_field, ENAME) | fields EMPNO, result |"
            + " head 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[2])\n"
            + "  LogicalProject(EMPNO=[$0], result=[COALESCE(null:NULL, $1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(NULL, `ENAME`) `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithMultipleNonExistentFields() {
    String ppl =
        "source=EMP | eval result = coalesce(field1, field2, ENAME, 'fallback') | fields EMPNO,"
            + " result | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], result=[COALESCE(null:NULL, null:NULL, $1,"
            + " 'fallback':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(NULL, NULL, `ENAME`, 'fallback') `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithAllNonExistentFields() {
    String ppl =
        "source=EMP | eval result = coalesce(field1, field2, field3) | fields EMPNO, result | head"
            + " 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], result=[COALESCE(null:NULL, null:NULL,"
            + " null:NULL)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(NULL, NULL, NULL) `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceNullWithInteger() {
    // Regression test for #5175: COALESCE(null, 42) should return INTEGER, not VARCHAR
    String ppl = "source=EMP | eval x = coalesce(null, 42) | fields EMPNO, x | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], x=[COALESCE(null:NULL, 42)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Verify the COALESCE return type is INTEGER, not VARCHAR
    org.apache.calcite.rel.logical.LogicalSort sort =
        (org.apache.calcite.rel.logical.LogicalSort) root;
    org.apache.calcite.rel.logical.LogicalProject proj =
        (org.apache.calcite.rel.logical.LogicalProject) sort.getInput();
    org.apache.calcite.rex.RexNode coalesceExpr = proj.getProjects().get(1);
    org.junit.Assert.assertEquals(
        org.apache.calcite.sql.type.SqlTypeName.INTEGER, coalesceExpr.getType().getSqlTypeName());
  }

  @Test
  public void testCoalesceIntegerWithNull() {
    // Regression test for #5175: COALESCE(42, null) should return INTEGER, not VARCHAR
    String ppl = "source=EMP | eval x = coalesce(42, null) | fields EMPNO, x | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], x=[COALESCE(42, null:NULL)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Verify the COALESCE return type is INTEGER, not VARCHAR
    org.apache.calcite.rel.logical.LogicalSort sort =
        (org.apache.calcite.rel.logical.LogicalSort) root;
    org.apache.calcite.rel.logical.LogicalProject proj =
        (org.apache.calcite.rel.logical.LogicalProject) sort.getInput();
    org.apache.calcite.rex.RexNode coalesceExpr = proj.getProjects().get(1);
    org.junit.Assert.assertEquals(
        org.apache.calcite.sql.type.SqlTypeName.INTEGER, coalesceExpr.getType().getSqlTypeName());
  }

  @Test
  public void testCoalesceNullWithIntegerResult() {
    // Regression test for #5175: COALESCE(null, 42) should return numeric 42, not string "42"
    String ppl = "source=EMP | eval x = coalesce(null, 42) | fields x | head 1";
    RelNode root = getRelNode(ppl);
    verifyResult(root, "x=42\n");
  }

  @Test
  public void testCoalesceWithEmptyString() {
    String ppl = "source=EMP | eval result = coalesce('', ENAME) | fields EMPNO, result | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], result=[COALESCE('':VARCHAR, $1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE('', `ENAME`) `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithSpaceString() {
    String ppl = "source=EMP | eval result = coalesce(' ', ENAME) | fields EMPNO, result | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], result=[COALESCE(' ', $1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(' ', `ENAME`) `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceTypeInferenceWithNonNullableOperands() {
    String ppl =
        "source=EMP | eval result = coalesce(COMM, SAL, 999) | fields EMPNO, COMM, SAL, result"
            + " | head 2";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[2])\n"
            + "  LogicalProject(EMPNO=[$0], COMM=[$6], SAL=[$5], result=[COALESCE($6, $5, 999)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `COMM`, `SAL`, COALESCE(`COMM`, `SAL`, 999) `result`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 2";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
