/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLScalarSubqueryTest extends CalcitePPLAbstractTest {

  public CalcitePPLScalarSubqueryTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testUncorrelatedScalarSubqueryInWhere() {
    String ppl =
        """
        source=EMP
        | where SAL > [
            source=EMP
            | stats AVG(SAL)
          ]
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalFilter(condition=[>($5, $SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], AVG(SAL)=[AVG($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "}))], variablesSet=[[$cor0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` > (((SELECT AVG(`SAL`) `AVG(SAL)`\n"
            + "FROM `scott`.`EMP`)))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testUncorrelatedScalarSubqueryInSelect() {
    String ppl =
        """
        source=EMP
        | eval min_empno = [
            source=EMP | stats min(EMPNO)
          ]
        | fields min_empno, SAL
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(variablesSet=[[$cor0]], min_empno=[$SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], min(EMPNO)=[MIN($0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT (((SELECT MIN(`EMPNO`) `min(EMPNO)`\n"
            + "FROM `scott`.`EMP`))) `min_empno`, `SAL`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testUncorrelatedScalarSubqueryInWhereAndSelect() {
    String ppl =
        """
        source=EMP
        | eval min_empno = [
            source=EMP | stats min(EMPNO)
          ]
        | where SAL > [
            source=EMP
            | stats AVG(SAL)
          ]
        | fields min_empno, SAL
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(min_empno=[$8], SAL=[$5])\n"
            + "  LogicalFilter(condition=[>($5, $SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], AVG(SAL)=[AVG($5)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "}))], variablesSet=[[$cor1]])\n"
            + "    LogicalProject(variablesSet=[[$cor0]], EMPNO=[$0], ENAME=[$1], JOB=[$2],"
            + " MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7],"
            + " min_empno=[$SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], min(EMPNO)=[MIN($0)])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "})])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `min_empno`, `SAL`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " (((SELECT MIN(`EMPNO`) `min(EMPNO)`\n"
            + "FROM `scott`.`EMP`))) `min_empno`\n"
            + "FROM `scott`.`EMP`) `t0`\n"
            + "WHERE `SAL` > (((SELECT AVG(`SAL`) `AVG(SAL)`\n"
            + "FROM `scott`.`EMP`)))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCorrelatedScalarSubqueryInWhere() {
    String ppl =
        """
        source=EMP
        | where SAL > [
            source=SALGRADE | where SAL = HISAL | stats AVG(SAL)
          ]
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalFilter(condition=[>($5, $SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], AVG(SAL)=[AVG($0)])\n"
            + "  LogicalProject($f3=[$cor0.SAL])\n"
            + "    LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "      LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "}))], variablesSet=[[$cor0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` > (((SELECT AVG(`EMP`.`SAL`) `AVG(SAL)`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`)))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCorrelatedScalarSubqueryInSelect() {
    String ppl =
        """
        source=EMP
        | eval min_empno = [
            source=SALGRADE | where SAL = HISAL | stats min(EMPNO)
          ]
        | fields min_empno, SAL
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(variablesSet=[[$cor0]], min_empno=[$SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], min(EMPNO)=[MIN($0)])\n"
            + "  LogicalProject($f3=[$cor0.EMPNO])\n"
            + "    LogicalFilter(condition=[=($cor0.SAL, $2)])\n"
            + "      LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT (((SELECT MIN(`EMP`.`EMPNO`) `min(EMPNO)`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL`))) `min_empno`, `SAL`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDisjunctiveCorrelatedScalarSubqueryInWhere() {
    String ppl =
        """
        source=EMP
        | where [
            source=SALGRADE | where SAL = HISAL OR HISAL > 1000.0 | stats COUNT()
          ] > 0
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalFilter(condition=[>($SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], COUNT()=[COUNT()])\n"
            + "  LogicalFilter(condition=[OR(=($cor0.SAL, $2), >($2, 1000.0E0:DOUBLE))])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "}), 0)], variablesSet=[[$cor0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (((SELECT COUNT(*) `COUNT()`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL` OR `HISAL` > 1.0000E3))) > 0";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDisjunctiveCorrelatedScalarSubqueryInWhere2() {
    String ppl =
        """
        source=EMP
        | where [
            source=SALGRADE | where (SAL = HISAL AND HISAL > 1000.0) OR (SAL = HISAL AND LOSAL > 1000.0) | stats COUNT()
          ] > 0
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalFilter(condition=[>($SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], COUNT()=[COUNT()])\n"
            + "  LogicalFilter(condition=[OR(AND(=($cor0.SAL, $2), >($2, 1000.0E0:DOUBLE)),"
            + " AND(=($cor0.SAL, $2), >($1, 1000.0E0:DOUBLE)))])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "}), 0)], variablesSet=[[$cor0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (((SELECT COUNT(*) `COUNT()`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `EMP`.`SAL` = `HISAL` AND `HISAL` > 1.0000E3 OR `EMP`.`SAL` = `HISAL` AND"
            + " `LOSAL` > 1.0000E3))) > 0";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTwoScalarSubqueriesInOr() {
    String ppl =
        """
        source=EMP
        | where SAL = [
            source=SALGRADE | sort LOSAL | stats max(HISAL)
          ] OR SAL = [
            source=SALGRADE | where LOSAL > 1000.0 | sort - HISAL | stats min(HISAL)
          ]
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalFilter(condition=[OR(=($5, $SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], max(HISAL)=[MAX($2)])\n"
            + "  LogicalSort(sort0=[$1], dir0=[ASC])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})), =($5, $SCALAR_QUERY({\n"
            + "LogicalAggregate(group=[{}], min(HISAL)=[MIN($2)])\n"
            + "  LogicalSort(sort0=[$2], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[>($1, 1000.0E0:DOUBLE)])\n"
            + "      LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "})))], variablesSet=[[$cor0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` = (((SELECT MAX(`HISAL`) `max(HISAL)`\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "ORDER BY `LOSAL` NULLS LAST) `t`))) OR `SAL` = (((SELECT MIN(`HISAL`) `min(HISAL)`\n"
            + "FROM (SELECT `GRADE`, `LOSAL`, `HISAL`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `LOSAL` > 1.0000E3\n"
            + "ORDER BY `HISAL` DESC NULLS FIRST) `t2`)))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedScalarSubquery() {
    String ppl =
        """
        source=EMP
        | where SAL = [
            source=SALGRADE
            | where HISAL = [
                source=EMP
                | stats max(SAL) as max_sal by JOB
                | fields max_sal
              ]
            | stats max(HISAL) as max_hisal by GRADE
            | fields max_hisal
            | head 1
          ]
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalFilter(condition=[=($5, $SCALAR_QUERY({\n"
            + "LogicalSort(fetch=[1])\n"
            + "  LogicalProject(max_hisal=[$1])\n"
            + "    LogicalAggregate(group=[{0}], max_hisal=[MAX($2)])\n"
            + "      LogicalFilter(condition=[=($2, $SCALAR_QUERY({\n"
            + "LogicalProject(max_sal=[$1])\n"
            + "  LogicalAggregate(group=[{2}], max_sal=[MAX($5)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "}))], variablesSet=[[$cor1]])\n"
            + "        LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "}))], variablesSet=[[$cor0]])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    // SELECT *
    // FROM scott.EMP
    // WHERE SAL = (
    //    SELECT MAX(HISAL) max_hisal
    //    FROM scott.SALGRADE
    //    WHERE HISAL = (
    //        SELECT MAX(SAL) max_sal
    //        FROM scott.EMP
    //        GROUP BY JOB
    //    )
    //    GROUP BY GRADE
    //    LIMIT 1
    // )
    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` = (((SELECT MAX(`HISAL`) `max_hisal`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `HISAL` = (((SELECT MAX(`SAL`) `max_sal`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`)))\n"
            + "GROUP BY `GRADE`\n"
            + "LIMIT 1)))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  // TODO: With Calcite, we can add more complex scalar subquery, such as
  // stats by a scalar subquery:
  // | eval count_a = [
  //     source=..
  //   ]
  // | stats .. by count_a
  // But currently, statsBy an expression is unsupported in PPL.
}
