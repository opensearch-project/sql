/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLAppendTest extends CalcitePPLAbstractTest {

  public CalcitePPLAppendTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testAppend() {
    String ppl = "source=EMP | append [ source=EMP | where DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 19); // 14 original table rows + 5 filtered subquery rows

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendEmptySearchCommand() {
    List<String> testPPLs =
        Arrays.asList("source=EMP | append [ | where DEPTNO = 20 ]", "source=EMP | append [ ]");
    for (String ppl : testPPLs) {
      RelNode root = getRelNode(ppl);
      String expectedLogical =
          "LogicalUnion(all=[true])\n"
              + "  LogicalTableScan(table=[[scott, EMP]])\n"
              + "  LogicalValues(tuples=[[]])\n";
      verifyLogical(root, expectedLogical);
      verifyResultCount(root, 14); // 14 original table rows

      String expectedSparkSql =
          "SELECT *\n"
              + "FROM `scott`.`EMP`\n"
              + "UNION ALL\n"
              + "SELECT *\n"
              + "FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) `t` (`EMPNO`,"
              + " `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`)\n"
              + "WHERE 1 = 0";
      verifyPPLToSparkSQL(root, expectedSparkSql);
    }
  }

  @Test
  public void testAppendDifferentIndex() {
    String ppl =
        "source=EMP | fields EMPNO, DEPTNO | append [ source=DEPT | fields DEPTNO, DNAME | where"
            + " DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(EMPNO=[$0], DEPTNO=[$7], DEPTNO0=[null:TINYINT],"
            + " DNAME=[null:VARCHAR(14)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(EMPNO=[null:SMALLINT], DEPTNO=[null:TINYINT], DEPTNO0=[$0],"
            + " DNAME=[$1])\n"
            + "    LogicalFilter(condition=[=($0, 20)])\n"
            + "      LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "        LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `DEPTNO`, CAST(NULL AS TINYINT) `DEPTNO0`, CAST(NULL AS STRING) `DNAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT CAST(NULL AS SMALLINT) `EMPNO`, CAST(NULL AS TINYINT) `DEPTNO`, `DEPTNO`"
            + " `DEPTNO0`, `DNAME`\n"
            + "FROM (SELECT `DEPTNO`, `DNAME`\n"
            + "FROM `scott`.`DEPT`) `t0`\n"
            + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendWithMergedColumns() {
    String ppl =
        "source=EMP | fields DEPTNO | append [ source=EMP | fields DEPTNO | eval DEPTNO_PLUS ="
            + " DEPTNO + 10 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], DEPTNO_PLUS=[null:INTEGER])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[$7], DEPTNO_PLUS=[+($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 28);

    String expectedSparkSql =
        "SELECT `DEPTNO`, CAST(NULL AS INTEGER) `DEPTNO_PLUS`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT `DEPTNO`, `DEPTNO` + 10 `DEPTNO_PLUS`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendWithConflictTypeColumn() {
    String ppl =
        "source=EMP | fields DEPTNO | append [ source=EMP | fields DEPTNO | eval DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(DEPTNO=[$7], DEPTNO0=[null:INTEGER])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(DEPTNO=[null:TINYINT], DEPTNO0=[20])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 28);

    String expectedSparkSql =
        "SELECT `DEPTNO`, CAST(NULL AS INTEGER) `DEPTNO0`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT CAST(NULL AS TINYINT) `DEPTNO`, 20 `DEPTNO0`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
