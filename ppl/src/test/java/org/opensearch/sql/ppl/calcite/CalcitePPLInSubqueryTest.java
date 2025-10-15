/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.SemanticCheckException;

public class CalcitePPLInSubqueryTest extends CalcitePPLAbstractTest {

  public CalcitePPLInSubqueryTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testInSubquery() {
    String ppl =
        "source=EMP | where DEPTNO in [ source=DEPT | fields DEPTNO ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[IN($7, {\n"
            + "LogicalProject(DEPTNO=[$0])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `scott`.`DEPT`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSelfInSubquery() {
    String ppl =
        "source=EMP | where MGR in [\n"
            + "    source=EMP\n"
            + "    | where DEPTNO = 10\n"
            + "    | fields MGR\n"
            + "  ]\n"
            + "| fields MGR\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(MGR=[$3])\n"
            + "  LogicalFilter(condition=[IN($3, {\n"
            + "LogicalProject(MGR=[$3])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `MGR`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `MGR` IN (SELECT `MGR`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 10)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTwoExpressionsInSubquery() {
    String ppl =
        "source=EMP | where (DEPTNO, ENAME) in [ source=DEPT | fields DEPTNO, DNAME ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[IN($7, $1, {\n"
            + "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO`, `ENAME`) IN (SELECT `DEPTNO`, `DNAME`\n"
            + "FROM `scott`.`DEPT`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterInSubquery() {
    String ppl =
        "source=EMP | where (DEPTNO, ENAME) in [ source=DEPT | fields DEPTNO, DNAME ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[IN($7, $1, {\n"
            + "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO`, `ENAME`) IN (SELECT `DEPTNO`, `DNAME`\n"
            + "FROM `scott`.`DEPT`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNotInSubquery() {
    String ppl =
        "source=EMP | where (DEPTNO, ENAME) not in [ source=DEPT | fields DEPTNO, DNAME ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[NOT(IN($7, $1, {\n"
            + "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "}))], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO`, `ENAME`) NOT IN (SELECT `DEPTNO`, `DNAME`\n"
            + "FROM `scott`.`DEPT`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedSubquery() {
    String ppl =
        "source=DEPT | where DEPTNO in [ source=EMP | where ENAME in [ source=BONUS | fields ENAME"
            + " ] | fields DEPTNO ]\n"
            + "| sort - DEPTNO | fields DNAME, LOC\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(DNAME=[$1], LOC=[$2])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[IN($0, {\n"
            + "LogicalProject(DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[IN($1, {\n"
            + "LogicalProject(ENAME=[$0])\n"
            + "  LogicalTableScan(table=[[scott, BONUS]])\n"
            + "})], variablesSet=[[$cor1]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `DNAME`, `LOC`\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IN (SELECT `ENAME`\n"
            + "FROM `scott`.`BONUS`))\n"
            + "ORDER BY `DEPTNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testInSubqueryAsJoinFilter() {
    String ppl =
        "source=EMP | inner join left=e, right=d\n"
            + "  ON e.DEPTNO = d.DEPTNO AND e.ENAME in [\n"
            + "    source=BONUS | WHERE SAL > 1000 | fields ENAME\n"
            + "  ]\n"
            + "  DEPT\n"
            + "| sort - e.EMPNO | fields e.EMPNO, e.ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "      LogicalJoin(condition=[AND(=($7, $8), IN($1, {\n"
            + "LogicalProject(ENAME=[$0])\n"
            + "  LogicalFilter(condition=[>($2, 1000)])\n"
            + "    LogicalTableScan(table=[[scott, BONUS]])\n"
            + "}))], joinType=[inner])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "        LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO` AND `EMP`.`ENAME` IN"
            + " (SELECT `ENAME`\n"
            + "FROM `scott`.`BONUS`\n"
            + "WHERE `SAL` > 1000)\n"
            + "ORDER BY `EMP`.`EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void failWhenNumOfColumnsNotMatchOutputOfSubquery() {
    String less =
        "source=EMP | where (DEPTNO) in [ source=DEPT | fields DEPTNO, DNAME ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    assertThrows(SemanticCheckException.class, () -> getRelNode(less));

    String more =
        "source=EMP | where (DEPTNO, ENAME) in [ source=DEPT | fields DEPTNO, DNAME, LOC ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    assertThrows(SemanticCheckException.class, () -> getRelNode(more));
  }

  @Test
  public void testInCorrelatedSubquery() {
    String ppl =
        "source=EMP | where ENAME in [\n"
            + "  source=DEPT | where EMP.DEPTNO = DEPTNO and LOC = 'BOSTON'| fields DNAME\n"
            + "]\n"
            + "| fields EMPNO, ENAME, DEPTNO\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[IN($1, {\n"
            + "LogicalProject(DNAME=[$1])\n"
            + "  LogicalFilter(condition=[AND(=($cor0.DEPTNO, $0), =($2, 'BOSTON':VARCHAR))])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IN (SELECT `DNAME`\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `EMP`.`DEPTNO` = `DEPTNO` AND `LOC` = 'BOSTON')";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSubsearchMaxOut() {
    doReturn(1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        "source=EMP | where DEPTNO in [ source=DEPT | fields DEPTNO ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[IN($7, {\n"
            + "LogicalSystemLimit(sort0=[$0], dir0=[ASC], fetch=[1], type=[SUBSEARCH_MAXOUT])\n"
            + "  LogicalProject(DEPTNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `scott`.`DEPT`\n"
            + "ORDER BY `DEPTNO` NULLS LAST\n"
            + "LIMIT 1)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testInCorrelatedSubqueryMaxOut() {
    doReturn(1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        "source=EMP | where ENAME in [\n"
            + "  source=DEPT | where EMP.DEPTNO = DEPTNO and LOC = 'BOSTON'| fields DNAME\n"
            + "]\n"
            + "| fields EMPNO, ENAME, DEPTNO\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[IN($1, {\n"
            + "LogicalProject(DNAME=[$1])\n"
            + "  LogicalFilter(condition=[=($cor0.DEPTNO, $0)])\n"
            + "    LogicalSystemLimit(sort0=[$0], dir0=[ASC], fetch=[1], type=[SUBSEARCH_MAXOUT])\n"
            + "      LogicalFilter(condition=[=($2, 'BOSTON':VARCHAR)])\n"
            + "        LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `ENAME` IN (SELECT `DNAME`\n"
            + "FROM (SELECT `DEPTNO`, `DNAME`, `LOC`\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `LOC` = 'BOSTON'\n"
            + "ORDER BY `DEPTNO` NULLS LAST\n"
            + "LIMIT 1) `t0`\n"
            + "WHERE `EMP`.`DEPTNO` = `DEPTNO`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
