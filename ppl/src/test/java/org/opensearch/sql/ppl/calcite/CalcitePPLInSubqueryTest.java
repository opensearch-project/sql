/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertThrows;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;
import org.opensearch.sql.exception.SemanticCheckException;

public class CalcitePPLInSubqueryTest extends CalcitePPLAbstractTest {

  public CalcitePPLInSubqueryTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testInSubquery() {
    String ppl =
        """
        source=EMP | where DEPTNO in [ source=DEPT | fields DEPTNO ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[IN($7, {\n"
            + "LogicalProject(DEPTNO=[$0])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IN (SELECT `DEPTNO`\n"
            + "FROM `scott`.`DEPT`)\n"
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSelfInSubquery() {
    String ppl =
        """
        source=EMP | where MGR in [
            source=EMP
            | where DEPTNO = 10
            | fields MGR
          ]
        | fields MGR
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(MGR=[$3])\n"
            + "  LogicalFilter(condition=[IN($3, {\n"
            + "LogicalProject(MGR=[$3])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "})])\n"
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
        """
        source=EMP | where (DEPTNO, ENAME) in [ source=DEPT | fields DEPTNO, DNAME ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[IN($7, $1, {\n"
            + "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO`, `ENAME`) IN (SELECT `DEPTNO`, `DNAME`\n"
            + "FROM `scott`.`DEPT`)\n"
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFilterInSubquery() {
    String ppl =
        """
        source=EMP (DEPTNO, ENAME) in [ source=DEPT | fields DEPTNO, DNAME ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[IN($7, $1, {\n"
            + "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "})])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO`, `ENAME`) IN (SELECT `DEPTNO`, `DNAME`\n"
            + "FROM `scott`.`DEPT`)\n"
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNotInSubquery() {
    String ppl =
        """
        source=EMP | where (DEPTNO, ENAME) not in [ source=DEPT | fields DEPTNO, DNAME ]
        | sort - EMPNO | fields EMPNO, ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[NOT(IN($7, $1, {\n"
            + "LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n"
            + "}))])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        ""
            + "SELECT `EMPNO`, `ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE (`DEPTNO`, `ENAME`) NOT IN (SELECT `DEPTNO`, `DNAME`\n"
            + "FROM `scott`.`DEPT`)\n"
            + "ORDER BY `EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNestedSubquery() {
    String ppl =
        """
        source=DEPT | where DEPTNO in [ source=EMP | where ENAME in [ source=BONUS | fields ENAME ] | fields DEPTNO ]
        | sort - DEPTNO | fields DNAME, LOC
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(DNAME=[$1], LOC=[$2])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalFilter(condition=[IN($0, {\n"
            + "LogicalProject(DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[IN($1, {\n"
            + "LogicalProject(ENAME=[$0])\n"
            + "  LogicalTableScan(table=[[scott, BONUS]])\n"
            + "})])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "})])\n"
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
            + "ORDER BY `DEPTNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testInSubqueryAsJoinFilter() {
    String ppl =
        """
        source=EMP | inner join left=e, right=d
          ON e.DEPTNO = d.DEPTNO AND e.ENAME in [
            source=BONUS | WHERE SAL > 1000 | fields ENAME
          ]
          DEPT
        | sort - e.EMPNO | fields e.EMPNO, e.ENAME
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[DESC])\n"
            + "    LogicalJoin(condition=[AND(=($7, $8), IN($1, {\n"
            + "LogicalProject(ENAME=[$0])\n"
            + "  LogicalFilter(condition=[>($2, 1000)])\n"
            + "    LogicalTableScan(table=[[scott, BONUS]])\n"
            + "}))], joinType=[inner])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO` AND `EMP`.`ENAME` IN"
            + " (SELECT `ENAME`\n"
            + "FROM `scott`.`BONUS`\n"
            + "WHERE `SAL` > 1000)\n"
            + "ORDER BY `EMP`.`EMPNO` DESC NULLS FIRST";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void failWhenNumOfColumnsNotMatchOutputOfSubquery() {
    String less =
        """
            source=EMP | where (DEPTNO) in [ source=DEPT | fields DEPTNO, DNAME ]
            | sort - EMPNO | fields EMPNO, ENAME
            """;
    assertThrows(SemanticCheckException.class, () -> getRelNode(less));

    String more =
        """
            source=EMP | where (DEPTNO, ENAME) in [ source=DEPT | fields DEPTNO, DNAME, LOC ]
            | sort - EMPNO | fields EMPNO, ENAME
            """;
    assertThrows(SemanticCheckException.class, () -> getRelNode(more));
  }
}
