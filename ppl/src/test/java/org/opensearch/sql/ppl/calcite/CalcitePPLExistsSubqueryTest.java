/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.mockito.Mockito.doReturn;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;

public class CalcitePPLExistsSubqueryTest extends CalcitePPLAbstractTest {
  public CalcitePPLExistsSubqueryTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testCorrelatedExistsSubqueryWithoutAlias() {
    String ppl =
        "source=EMP\n"
            + "| where exists [\n"
            + "    source=SALGRADE\n"
            + "    | where SAL = HISAL\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCorrelatedExistsSubqueryWithAlias() {
    String ppl =
        "source=EMP\n"
            + "| where exists [\n"
            + "    source=SALGRADE\n"
            + "    | where EMP.SAL = HISAL\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testUncorrelatedExistsSubquery() {
    String ppl =
        "source=EMP\n"
            + "| where exists [\n"
            + "    source=DEPT\n"
            + "    | where DNAME = 'Accounting'\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($1, 'Accounting':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `DNAME` = 'Accounting')\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCorrelatedNotExistsSubqueryWithoutAlias() {
    String ppl =
        "source=EMP\n"
            + "| where not exists [\n"
            + "    source=SALGRADE\n"
            + "    | where SAL = HISAL\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[NOT(EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "}))], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE NOT EXISTS (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCorrelatedNotExistsSubqueryWithTableNames() {
    String ppl =
        "source=EMP\n"
            + "| where not exists [\n"
            + "    source=SALGRADE\n"
            + "    | where EMP.SAL = SALGRADE.HISAL\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[NOT(EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "}))], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE NOT EXISTS (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testUncorrelatedNotExistsSubquery() {
    String ppl =
        "source=EMP\n"
            + "| where not exists [\n"
            + "    source=DEPT\n"
            + "    | where DNAME = 'Accounting'\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[NOT(EXISTS({\n"
            + "LogicalFilter(condition=[=($1, 'Accounting':VARCHAR)])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "}))], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE NOT EXISTS (SELECT *\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `DNAME` = 'Accounting')\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testExistsSubqueryInFilter() {
    String ppl =
        "source=EMP | where exists [\n"
            + "    source=SALGRADE\n"
            + "    | where SAL = HISAL\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNotExistsSubqueryInFilter() {
    String ppl =
        "source=EMP | where not exists [\n"
            + "    source=SALGRADE\n"
            + "    | where SAL = HISAL\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[NOT(EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "}))], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE NOT EXISTS (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedCorrelatedExistsSubquery() {
    String ppl =
        "source=EMP as outer\n"
            + "| where exists [\n"
            + "    source=SALGRADE as inner\n"
            + "    | where exists [\n"
            + "        source=EMP as nested\n"
            + "        | where nested.SAL = inner.HISAL\n"
            + "      ]\n"
            + "    | where outer.SAL = inner.HISAL\n"
            + "  ]\n"
            + "| sort - outer.EMPNO | fields outer.EMPNO, outer.ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($5, $cor1.HISAL)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor1]])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` = `SALGRADE`.`HISAL`)) `t0`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedUncorrelatedExistsSubquery() {
    String ppl =
        "source=EMP as outer\n"
            + "| where exists [\n"
            + "    source=SALGRADE as inner\n"
            + "    | where exists [\n"
            + "        source=EMP as nested\n"
            + "        | where nested.SAL > 1000.0\n"
            + "      ]\n"
            + "    | where inner.HISAL > 1000.0\n"
            + "  ]\n"
            + "| sort - outer.EMPNO | fields outer.EMPNO, outer.ENAME\n";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[>($2, 1000.0:DECIMAL(5, 1))])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[>($5, 1000.0:DECIMAL(5, 1))])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor1]])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` > 1000.0)) `t0`\n"
            + "WHERE `HISAL` > 1000.0)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedMixedExistsSubquery() {
    String ppl =
        "source=EMP as e1\n"
            + "| where exists [\n"
            + "    source=SALGRADE as s2\n"
            + "    | where exists [\n"
            + "        source=EMP as e3\n"
            + "        | where exists [\n"
            + "          source=SALGRADE as s4\n"
            + "          | where exists [\n"
            + "              source=EMP\n"
            + "              | where SAL = s4.HISAL\n"
            + "            ]\n"
            + "          | where s3.SAL > 1000.0\n"
            + "        ]\n"
            + "        | where SAL = s2.HISAL\n"
            + "      ]\n"
            + "    | where e1.SAL > 1000.0\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode plan1 = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[>($cor0.SAL, 1000.0:DECIMAL(5, 1))])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($5, $cor1.HISAL)])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[>($cor2.SAL, 1000.0:DECIMAL(5, 1))])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($5, $cor3.HISAL)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor3]])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor2]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor1]])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(plan1, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` = `SALGRADE0`.`HISAL`)) `t0`\n"
            + "WHERE `EMP0`.`SAL` > 1000.0)) `t2`\n"
            + "WHERE `SAL` = `SALGRADE`.`HISAL`)) `t4`\n"
            + "WHERE `EMP`.`SAL` > 1000.0)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(plan1, expectedSparkSql);

    String pplWithoutAlias =
        "source=EMP\n"
            + "| where exists [\n"
            + "    source=SALGRADE\n"
            + "    | where exists [\n"
            + "        source=EMP\n"
            + "        | where exists [\n"
            + "          source=SALGRADE\n"
            + "          | where exists [\n"
            + "              source=EMP\n"
            + "              | where SAL = HISAL\n"
            + "            ]\n"
            + "          | where SAL > 1000.0\n"
            + "        ]\n"
            + "        | where SAL = HISAL\n"
            + "      ]\n"
            + "    | where SAL > 1000.0\n"
            + "  ]\n"
            + "| sort - EMPNO | fields EMPNO, ENAME\n";
    RelNode plan2 = getRelNode(pplWithoutAlias);
    verifyLogical(plan2, expectedLogical);
  }

  @Test
  public void testCorrelatedExistsSubqueryWithOverridingFields() {
    String overriding =
        "source=EMP | eval DEPTNO = DEPTNO + 1\n"
            + "| where exists [\n"
            + "    source=DEPT\n"
            + "    | where emp.DEPTNO = DEPTNO\n"
            + "  ]\n";
    RelNode root = getRelNode(overriding);
    String expectedLogical =
        "LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.DEPTNO, $0)])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[+($7, 1)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO` + 1"
            + " `DEPTNO`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `t`.`DEPTNO` = `DEPTNO`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSubsearchMaxOut1() {
    doReturn(1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | where EMP.SAL = HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalSystemLimit(sort0=[$0], dir0=[ASC], fetch=[1], type=[SUBSEARCH_MAXOUT])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "ORDER BY `GRADE` NULLS LAST\n"
            + "LIMIT 1) `t`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSubsearchMaxOut2() {
    doReturn(1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | where EMP.SAL = HISAL and LOSAL > 1000.0
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "  LogicalSystemLimit(sort0=[$0], dir0=[ASC], fetch=[1], type=[SUBSEARCH_MAXOUT])\n"
            + "    LogicalFilter(condition=[>($1, 1000.0:DECIMAL(5, 1))])\n"
            + "      LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `LOSAL` > 1000.0\n"
            + "ORDER BY `GRADE` NULLS LAST\n"
            + "LIMIT 1) `t0`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSubsearchMaxOut3() {
    doReturn(1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | where EMP.SAL = HISAL
            | eval LOSAL1 = LOSAL
            | where LOSAL > 1000.0
            | sort - HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalSort(sort0=[$2], dir0=[DESC-nulls-last])\n"
            + "  LogicalFilter(condition=[>($1, 1000.0:DECIMAL(5, 1))])\n"
            + "    LogicalProject(GRADE=[$0], LOSAL=[$1], HISAL=[$2], LOSAL1=[$1])\n"
            + "      LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "        LogicalSystemLimit(sort0=[$0], dir0=[ASC], fetch=[1],"
            + " type=[SUBSEARCH_MAXOUT])\n"
            + "          LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT `GRADE`, `LOSAL`, `HISAL`, `LOSAL1`\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`, `LOSAL` `LOSAL1`\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "ORDER BY `GRADE` NULLS LAST\n"
            + "LIMIT 1) `t`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`) `t1`\n"
            + "WHERE `LOSAL` > 1000.0\n"
            + "ORDER BY `HISAL` DESC)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSubsearchMaxOutUncorrelated1() {
    doReturn(1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | eval LOSAL1 = LOSAL
            | where LOSAL > 1000.0
            | sort - HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalSystemLimit(sort0=[$2], dir0=[DESC-nulls-last], fetch=[1],"
            + " type=[SUBSEARCH_MAXOUT])\n"
            + "  LogicalSort(sort0=[$2], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[>($1, 1000.0:DECIMAL(5, 1))])\n"
            + "      LogicalProject(GRADE=[$0], LOSAL=[$1], HISAL=[$2], LOSAL1=[$1])\n"
            + "        LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT `GRADE`, `LOSAL`, `HISAL`, `LOSAL1`\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`, `LOSAL1`\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`, `LOSAL` `LOSAL1`\n"
            + "FROM `scott`.`SALGRADE`) `t`\n"
            + "WHERE `LOSAL` > 1000.0\n"
            + "ORDER BY `HISAL` DESC) `t1`\n"
            + "ORDER BY `HISAL` DESC\n"
            + "LIMIT 1)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSubsearchMaxOutUncorrelated2() {
    doReturn(1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | join type=left LOSAL SALGRADE
            | eval LOSAL1 = LOSAL
            | where LOSAL > 1000.0
            | sort - HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalSystemLimit(sort0=[$2], dir0=[DESC-nulls-last], fetch=[1],"
            + " type=[SUBSEARCH_MAXOUT])\n"
            + "  LogicalSort(sort0=[$2], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[>($1, 1000.0:DECIMAL(5, 1))])\n"
            + "      LogicalProject(GRADE=[$3], LOSAL=[$4], HISAL=[$5], LOSAL1=[$4])\n"
            + "        LogicalJoin(condition=[=($1, $4)], joinType=[left])\n"
            + "          LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "          LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT `GRADE`, `LOSAL`, `HISAL`, `LOSAL1`\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`, `LOSAL1`\n"
            + "FROM (SELECT `SALGRADE0`.`GRADE`, `SALGRADE0`.`LOSAL`, `SALGRADE0`.`HISAL`,"
            + " `SALGRADE0`.`LOSAL` `LOSAL1`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "LEFT JOIN `scott`.`SALGRADE` `SALGRADE0` ON `SALGRADE`.`LOSAL` ="
            + " `SALGRADE0`.`LOSAL`) `t`\n"
            + "WHERE `t`.`LOSAL` > 1000.0\n"
            + "ORDER BY `HISAL` DESC) `t1`\n"
            + "ORDER BY `HISAL` DESC\n"
            + "LIMIT 1)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSubsearchMaxOutZero() {
    doReturn(0).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | where EMP.SAL = HISAL and LOSAL > 1000.0
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalValues(tuples=[[]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM (VALUES (NULL, NULL, NULL)) `t` (`GRADE`, `LOSAL`, `HISAL`)\n"
            + "WHERE 1 = 0)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSubsearchMaxOutUnlimited() {
    doReturn(-1).when(settings).getSettingValue(Settings.Key.PPL_SUBSEARCH_MAXOUT);
    String ppl =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | where EMP.SAL = HISAL and LOSAL > 1000.0
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[AND(=($cor0.SAL, $2), >($1, 1000.0:DECIMAL(5, 1)))])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT *\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL` AND `LOSAL` > 1000.0)\n"
            + "ORDER BY `EMPNO` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
