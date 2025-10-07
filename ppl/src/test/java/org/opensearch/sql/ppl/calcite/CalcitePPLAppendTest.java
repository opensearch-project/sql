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
    List<String> emptySourcePPLs =
        Arrays.asList(
            "source=EMP | append [ | where DEPTNO = 20 ]",
            "source=EMP | append [ ]",
            "source=EMP | append [ | where DEPTNO = 20 | append [ ] ]",
            "source=EMP | append [ | where DEPTNO = 10 | lookup DEPT DEPTNO append LOC as JOB ]");

    for (String ppl : emptySourcePPLs) {
      RelNode root = getRelNode(ppl);
      String expectedLogical =
          "LogicalUnion(all=[true])\n"
              + "  LogicalTableScan(table=[[scott, EMP]])\n"
              + "  LogicalValues(tuples=[[]])\n";
      verifyLogical(root, expectedLogical);

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
  public void testAppendNested() {
    String ppl =
        "source=EMP | append [ | where DEPTNO = 10 | append [ source=EMP | where DEPTNO = 20 ] ]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], EMPNO0=[null:SMALLINT])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalProject(EMPNO=[null:SMALLINT], ENAME=[$1], JOB=[$2], MGR=[$3],"
            + " HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], EMPNO0=[$0])\n"
            + "    LogicalUnion(all=[true])\n"
            + "      LogicalValues(tuples=[[]])\n"
            + "      LogicalFilter(condition=[=($7, 20)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 19); // 14 original table rows + 5 filtered subquery rows

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, CAST(NULL AS"
            + " SMALLINT) `EMPNO0`\n"
            + "FROM `scott`.`EMP`\n"
            + "UNION ALL\n"
            + "SELECT CAST(NULL AS SMALLINT) `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`,"
            + " `COMM`, `DEPTNO`, `EMPNO` `EMPNO0`\n"
            + "FROM (SELECT *\n"
            + "FROM (VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) `t` (`EMPNO`,"
            + " `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`)\n"
            + "WHERE 1 = 0\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20) `t2`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testAppendEmptySourceWithJoin() {
    List<String> emptySourceWithEmptySourceJoinPPLs =
        Arrays.asList(
            "source=EMP | append [ | where DEPTNO = 10 | join on ENAME = DNAME DEPT ]",
            "source=EMP | append [ | where DEPTNO = 10 | cross join on ENAME = DNAME DEPT ]",
            "source=EMP | append [ | where DEPTNO = 10 | left join on ENAME = DNAME DEPT ]",
            "source=EMP | append [ | where DEPTNO = 10 | semi join on ENAME = DNAME DEPT ]",
            "source=EMP | append [ | where DEPTNO = 10 | anti join on ENAME = DNAME DEPT ]");

    for (String ppl : emptySourceWithEmptySourceJoinPPLs) {
      RelNode root = getRelNode(ppl);
      String expectedLogical =
          "LogicalUnion(all=[true])\n"
              + "  LogicalTableScan(table=[[scott, EMP]])\n"
              + "  LogicalValues(tuples=[[]])\n";
      verifyLogical(root, expectedLogical);
      verifyResultCount(root, 14);

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

    List<String> emptySourceWithRightOrFullJoinPPLs =
        Arrays.asList(
            "source=EMP | append [ | where DEPTNO = 10 | right join on ENAME = DNAME DEPT ]",
            "source=EMP | append [ | where DEPTNO = 10 | full join on ENAME = DNAME DEPT ]");

    for (String ppl : emptySourceWithRightOrFullJoinPPLs) {
      RelNode root = getRelNode(ppl);
      String expectedLogical =
          "LogicalUnion(all=[true])\n"
              + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
              + " SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPTNO0=[null:TINYINT],"
              + " DNAME=[null:VARCHAR(14)], LOC=[null:VARCHAR(13)])\n"
              + "    LogicalTableScan(table=[[scott, EMP]])\n"
              + "  LogicalProject(EMPNO=[null:SMALLINT], ENAME=[null:VARCHAR(10)],"
              + " JOB=[null:VARCHAR(9)], MGR=[null:SMALLINT], HIREDATE=[null:DATE],"
              + " SAL=[null:DECIMAL(7, 2)], COMM=[null:DECIMAL(7, 2)], DEPTNO=[null:TINYINT],"
              + " DEPTNO0=[$0], DNAME=[$1], LOC=[$2])\n"
              + "    LogicalTableScan(table=[[scott, DEPT]])\n";
      verifyLogical(root, expectedLogical);

      String expectedSparkSql =
          "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`, CAST(NULL AS"
              + " TINYINT) `DEPTNO0`, CAST(NULL AS STRING) `DNAME`, CAST(NULL AS STRING) `LOC`\n"
              + "FROM `scott`.`EMP`\n"
              + "UNION ALL\n"
              + "SELECT CAST(NULL AS SMALLINT) `EMPNO`, CAST(NULL AS STRING) `ENAME`, CAST(NULL AS"
              + " STRING) `JOB`, CAST(NULL AS SMALLINT) `MGR`, CAST(NULL AS DATE) `HIREDATE`,"
              + " CAST(NULL AS DECIMAL(7, 2)) `SAL`, CAST(NULL AS DECIMAL(7, 2)) `COMM`, CAST(NULL"
              + " AS TINYINT) `DEPTNO`, `DEPTNO` `DEPTNO0`, `DNAME`, `LOC`\n"
              + "FROM `scott`.`DEPT`";
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
