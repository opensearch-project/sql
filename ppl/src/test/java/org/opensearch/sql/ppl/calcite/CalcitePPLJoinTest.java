/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
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
}
