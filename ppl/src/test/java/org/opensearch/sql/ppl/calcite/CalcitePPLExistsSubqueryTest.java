/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLExistsSubqueryTest extends CalcitePPLAbstractTest {
  public CalcitePPLExistsSubqueryTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testCorrelatedExistsSubqueryWithoutAlias() {
    String ppl =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | where SAL = HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCorrelatedExistsSubqueryWithAlias() {
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
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testUncorrelatedExistsSubquery() {
    String ppl =
        """
        source=EMP
        | where exists [
            source=DEPT
            | where DNAME = 'Accounting'
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($1, 'Accounting')])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCorrelatedNotExistsSubqueryWithoutAlias() {
    String ppl =
        """
        source=EMP
        | where not exists [
            source=SALGRADE
            | where SAL = HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCorrelatedNotExistsSubqueryWithTableNames() {
    String ppl =
        """
        source=EMP
        | where not exists [
            source=SALGRADE
            | where EMP.SAL = SALGRADE.HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testUncorrelatedNotExistsSubquery() {
    String ppl =
        """
        source=EMP
        | where not exists [
            source=DEPT
            | where DNAME = 'Accounting'
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[NOT(EXISTS({\n"
            + "LogicalFilter(condition=[=($1, 'Accounting')])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testExistsSubqueryInFilter() {
    String ppl =
        """
        source=EMP exists [
            source=SALGRADE
            | where SAL = HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNotExistsSubqueryInFilter() {
    String ppl =
        """
        source=EMP not exists [
            source=SALGRADE
            | where SAL = HISAL
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedCorrelatedExistsSubquery() {
    String ppl =
        """
        source=EMP as outer
        | where exists [
            source=SALGRADE as inner
            | where exists [
                source=EMP as nested
                | where nested.SAL = inner.HISAL
              ]
            | where outer.SAL = inner.HISAL
          ]
        | sort - outer.EMPNO | fields outer.EMPNO, outer.ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
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
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedUncorrelatedExistsSubquery() {
    String ppl =
        """
        source=EMP as outer
        | where exists [
            source=SALGRADE as inner
            | where exists [
                source=EMP as nested
                | where nested.SAL > 1000.0
              ]
            | where inner.HISAL > 1000.0
          ]
        | sort - outer.EMPNO | fields outer.EMPNO, outer.ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[>($2, 1000.0E0:DOUBLE)])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[>($5, 1000.0E0:DOUBLE)])\n"
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
            + "WHERE `SAL` > 1.0000E3)) `t0`\n"
            + "WHERE `HISAL` > 1.0000E3)\n"
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedMixedExistsSubquery() {
    String ppl =
        """
        source=EMP as e1
        | where exists [
            source=SALGRADE as s2
            | where exists [
                source=EMP as e3
                | where exists [
                  source=SALGRADE as s4
                  | where exists [
                      source=EMP
                      | where SAL = s4.HISAL
                    ]
                  | where s3.SAL > 1000.0
                ]
                | where SAL = s2.HISAL
              ]
            | where e1.SAL > 1000.0
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode plan1 = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[>($cor0.SAL, 1000.0E0:DOUBLE)])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[=($5, $cor1.HISAL)])\n"
            + "  LogicalFilter(condition=[EXISTS({\n"
            + "LogicalFilter(condition=[>($cor2.SAL, 1000.0E0:DOUBLE)])\n"
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
            + "WHERE `EMP0`.`SAL` > 1.0000E3)) `t2`\n"
            + "WHERE `SAL` = `SALGRADE`.`HISAL`)) `t4`\n"
            + "WHERE `EMP`.`SAL` > 1.0000E3)\n"
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(plan1, expectedSparkSql);

    String pplWithoutAlias =
        """
        source=EMP
        | where exists [
            source=SALGRADE
            | where exists [
                source=EMP
                | where exists [
                  source=SALGRADE
                  | where exists [
                      source=EMP
                      | where SAL = HISAL
                    ]
                  | where SAL > 1000.0
                ]
                | where SAL = HISAL
              ]
            | where SAL > 1000.0
          ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode plan2 = getRelNode(pplWithoutAlias);
    verifyLogical(plan2, expectedLogical);
  }

  @Test
  public void testCorrelatedExistsSubqueryWithOverridingFields() {
    String overriding =
        """
        source=EMP | eval DEPTNO = DEPTNO + 1
        | where exists [
            source=DEPT
            | where emp.DEPTNO = DEPTNO
          ]
        """;
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
}
