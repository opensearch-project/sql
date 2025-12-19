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
            + "  LogicalProject(EMPNO=[$0], COMM=[$6], result=[COALESCE($6, $0)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `COMM`, COALESCE(`COMM`, `EMPNO`) `result`\n"
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
            + "  LogicalProject(EMPNO=[$0], result=[COALESCE($6, 123)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(`COMM`, 123) `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
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
            + " result2=[COALESCE($6, 0)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `COMM`, `SAL`, COALESCE(`COMM`, 0) `result1`, COALESCE(`COMM`, 0)"
            + " `result2`\n"
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
            + "  LogicalProject(EMPNO=[$0], result=[$1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME` `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 2";
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
            + "  LogicalProject(EMPNO=[$0], result=[COALESCE($1, 'fallback':VARCHAR)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, COALESCE(`ENAME`, 'fallback') `result`\n"
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
            + "  LogicalProject(EMPNO=[$0], result=[null:VARCHAR])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, CAST(NULL AS STRING) `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithEmptyString() {
    String ppl = "source=EMP | eval result = coalesce('', ENAME) | fields EMPNO, result | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], result=['':VARCHAR])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, '' `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCoalesceWithSpaceString() {
    String ppl = "source=EMP | eval result = coalesce(' ', ENAME) | fields EMPNO, result | head 1";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(EMPNO=[$0], result=[' '])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, ' ' `result`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 1";
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
