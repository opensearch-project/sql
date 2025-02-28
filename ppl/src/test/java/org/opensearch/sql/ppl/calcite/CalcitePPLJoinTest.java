/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLJoinTest extends CalcitePPLAbstractTest {

  public CalcitePPLJoinTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testJoinConditionWithTableNames() {
    String ppl = "source=EMP | join on EMP.DEPTNO = DEPT.DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinConditionWithAlias() {
    String ppl = "source=EMP as e | join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinConditionWithoutTableName() {
    String ppl = "source=EMP | join on ENAME = DNAME DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($1, $9)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 0);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`ENAME` = `DEPT`.`DNAME`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithSpecificAliases() {
    String ppl = "source=EMP | join left = l right = r on l.DEPTNO = r.DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithMultiplePredicates() {
    String ppl =
        "source=EMP | join left = l right = r on l.DEPTNO = r.DEPTNO AND l.DEPTNO > 10 AND EMP.SAL"
            + " < 3000 DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[AND(=($7, $8), >($7, 10), <($5, 3000))], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 9);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO` AND `EMP`.`DEPTNO` >"
            + " 10 AND `EMP`.`SAL` < 3000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLeftJoin() {
    String ppl = "source=EMP as e | left join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRightJoin() {
    String ppl = "source=EMP as e | right join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[right])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 15);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "RIGHT JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLeftSemi() {
    String ppl = "source=EMP as e | left semi join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[semi])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE EXISTS (SELECT 1\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testLeftAnti() {
    String ppl = "source=EMP as e | left anti join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[anti])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 0);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE NOT EXISTS (SELECT 1\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFullOuter() {
    String ppl = "source=EMP as e | full outer join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[full])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 15);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "FULL JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCrossJoin() {
    String ppl = "source=EMP as e | cross join DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[true], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 56);

    String expectedSparkSql =
        "" + "SELECT *\n" + "FROM `scott`.`EMP`\n" + "CROSS JOIN `scott`.`DEPT`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCrossJoinWithJoinConditions() {
    String ppl = "source=EMP as e | cross join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNonEquiJoin() {
    String ppl = "source=EMP as e | join on e.DEPTNO > d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[>($7, $8)], joinType=[inner])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 17);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` > `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleTablesJoin() {
    String ppl =
        "source=EMP | join left = l right = r ON l.DEPTNO = r.DEPTNO DEPT | left join left = l"
            + " right = r ON l.SAL = r.HISAL SALGRADE";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `EMP`.`SAL` = `SALGRADE`.`HISAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleTablesJoinWithTableAliases() {
    String ppl =
        "source=EMP as t1 | join ON t1.DEPTNO = t2.DEPTNO DEPT as t2 | left join ON t1.SAL ="
            + " t3.HISAL SALGRADE as t3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `EMP`.`SAL` = `SALGRADE`.`HISAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleTablesJoinWithTableNames() {
    String ppl =
        "source=EMP | join ON EMP.DEPTNO = DEPT.DEPTNO DEPT | left join ON EMP.SAL = SALGRADE.HISAL"
            + " SALGRADE";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `EMP`.`SAL` = `SALGRADE`.`HISAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleJoinWithPartSideAliases() {
    String ppl =
        "source=EMP | join left = t1 right = t2 ON t1.DEPTNO = t2.DEPTNO DEPT | left join right ="
            + " t3 ON t1.SAL = t3.HISAL SALGRADE";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        ""
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `EMP`.`SAL` = `SALGRADE`.`HISAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleJoinWithSelfJoin() {
    String ppl =
        "source=EMP | join left = t1 right = t2 ON t1.DEPTNO = t2.DEPTNO DEPT | left join right ="
            + " t3 ON t1.SAL = t3.HISAL SALGRADE | join right = t4 ON t1.DEPTNO = t4.DEPTNO EMP |"
            + " fields t1.ENAME, t2.DNAME, t3.GRADE, t4.EMPNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalProject(ENAME=[$1], DNAME=[$9], GRADE=[$11], EMPNO=[$14])\n"
            + "  LogicalJoin(condition=[=($7, $21)], joinType=[inner])\n"
            + "    LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "      LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "        LogicalTableScan(table=[[scott, DEPT]])\n"
            + "      LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 70);

    String expectedSparkSql =
        ""
            + "SELECT `EMP`.`ENAME`, `DEPT`.`DNAME`, `SALGRADE`.`GRADE`, `EMP0`.`EMPNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `EMP`.`SAL` = `SALGRADE`.`HISAL`\n"
            + "INNER JOIN `scott`.`EMP` `EMP0` ON `EMP`.`DEPTNO` = `EMP0`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
