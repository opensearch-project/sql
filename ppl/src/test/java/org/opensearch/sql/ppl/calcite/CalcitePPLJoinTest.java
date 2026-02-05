/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.mockito.Mockito.doReturn;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.SemanticCheckException;

public class CalcitePPLJoinTest extends CalcitePPLAbstractTest {

  public CalcitePPLJoinTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testJoinConditionWithTableNames() {
    String ppl = "source=EMP | join on EMP.DEPTNO = DEPT.DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], DEPT.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `DEPT.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinConditionWithAlias() {
    String ppl =
        "source=EMP as e | join on e.DEPTNO = d.DEPTNO DEPT as d | where LOC = 'CHICAGO' | fields"
            + " d.DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(d.DEPTNO=[$8])\n"
            + "  LogicalFilter(condition=[=($10, 'CHICAGO':VARCHAR)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "      LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "        LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 6);

    String expectedSparkSql =
        "SELECT `d.DEPTNO`\n"
            + "FROM (SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`,"
            + " `EMP`.`HIREDATE`, `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO`"
            + " `d.DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`) `t`\n"
            + "WHERE `t`.`LOC` = 'CHICAGO'";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinConditionWithoutTableName() {
    String ppl = "source=EMP | join on ENAME = DNAME DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], DEPT.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($1, $9)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 0);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `DEPT.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`ENAME` = `DEPT`.`DNAME`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithSpecificAliases() {
    String ppl = "source=EMP | join left = l right = r on l.DEPTNO = r.DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `r.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
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
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[AND(=($7, $8), >($7, 10), <($5, 3000))],"
            + " joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 9);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `r.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
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
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `d.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testRightJoin() {
    String ppl = "source=EMP as e | right join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[right])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 15);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `d.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
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
            + "LEFT SEMI JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
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
            + "LEFT ANTI JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFullOuter() {
    String ppl = "source=EMP as e | full outer join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[full])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 15);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `d.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "FULL JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCrossJoin() {
    String ppl = "source=EMP as e | cross join on 1=1 DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[true], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 56);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `d.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "CROSS JOIN `scott`.`DEPT`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCrossJoinWithJoinConditions() {
    String ppl = "source=EMP as e | cross join on e.DEPTNO = d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `d.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testNonEquiJoin() {
    String ppl = "source=EMP as e | join on e.DEPTNO > d.DEPTNO DEPT as d";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[>($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 17);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `d.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
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
        "LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "    LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`,"
            + " `EMP`.`HIREDATE`, `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO`"
            + " `r.DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`) `t`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `t`.`SAL` = `SALGRADE`.`HISAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleTablesJoinWithTableAliases() {
    String ppl =
        "source=EMP as t1 | join ON t1.DEPTNO = t2.DEPTNO DEPT as t2 | left join ON t1.SAL ="
            + " t3.HISAL SALGRADE as t3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], t2.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "    LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`,"
            + " `EMP`.`HIREDATE`, `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO`"
            + " `t2.DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`) `t`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `t`.`SAL` = `SALGRADE`.`HISAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleTablesJoinWithTableNames() {
    String ppl =
        "source=EMP | join ON EMP.DEPTNO = DEPT.DEPTNO DEPT | left join ON EMP.SAL = SALGRADE.HISAL"
            + " SALGRADE";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPT.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "    LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`,"
            + " `EMP`.`HIREDATE`, `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO`"
            + " `DEPT.DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`) `t`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `t`.`SAL` = `SALGRADE`.`HISAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleJoinWithPartSideAliases() {
    String ppl =
        "source=EMP | join left = t1 right = t2 ON t1.DEPTNO = t2.DEPTNO DEPT | left join right ="
            + " t3 ON t1.SAL = t3.HISAL SALGRADE";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], t2.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "    LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n"
            + "  LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`,"
            + " `EMP`.`HIREDATE`, `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO`"
            + " `t2.DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`) `t`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `t`.`SAL` = `SALGRADE`.`HISAL`";
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
        "LogicalProject(ENAME=[$1], DNAME=[$9], GRADE=[$11], t4.EMPNO=[$14])\n"
            + "  LogicalJoin(condition=[=($7, $21)], joinType=[inner])\n"
            + "    LogicalJoin(condition=[=($5, $13)], joinType=[left])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], t2.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "        LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n"
            + "          LogicalTableScan(table=[[scott, DEPT]])\n"
            + "      LogicalTableScan(table=[[scott, SALGRADE]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 70);

    String expectedSparkSql =
        "SELECT `t`.`ENAME`, `t`.`DNAME`, `SALGRADE`.`GRADE`, `EMP0`.`EMPNO` `t4.EMPNO`\n"
            + "FROM (SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`,"
            + " `EMP`.`HIREDATE`, `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO`"
            + " `t2.DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`) `t`\n"
            + "LEFT JOIN `scott`.`SALGRADE` ON `t`.`SAL` = `SALGRADE`.`HISAL`\n"
            + "INNER JOIN `scott`.`EMP` `EMP0` ON `t`.`DEPTNO` = `EMP0`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  // +-----------------------------+
  // | join with relation subquery |
  // +-----------------------------+

  @Test
  public void testJoinWithRelationSubquery() {
    String ppl =
        """
        source=EMP | join left = t1 right = t2 ON t1.DEPTNO = t2.DEPTNO
          [
            source = DEPT
            | where DEPTNO > 10 and LOC = 'CHICAGO'
            | fields DEPTNO, DNAME
            | sort - DEPTNO
            | head 10
          ]
        | stats sum(MGR) as sum by JOB
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(sum=[$1], JOB=[$0])\n"
            + "  LogicalAggregate(group=[{0}], sum=[SUM($1)])\n"
            + "    LogicalProject(JOB=[$2], MGR=[$3])\n"
            + "      LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "        LogicalSort(sort0=[$0], dir0=[DESC-nulls-last], fetch=[10])\n"
            + "          LogicalProject(DEPTNO=[$0], DNAME=[$1])\n"
            + "            LogicalFilter(condition=[AND(>($0, 10), =($2, 'CHICAGO':VARCHAR))])\n"
            + "              LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "sum=30792; JOB=SALESMAN\nsum=7698; JOB=CLERK\nsum=7839; JOB=MANAGER\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql =
        ""
            + "SELECT SUM(`EMP`.`MGR`) `sum`, `EMP`.`JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN (SELECT `DEPTNO`, `DNAME`\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `DEPTNO` > 10 AND `LOC` = 'CHICAGO'\n"
            + "ORDER BY `DEPTNO` DESC\n"
            + "LIMIT 10) `t1` ON `EMP`.`DEPTNO` = `t1`.`DEPTNO`\n"
            + "GROUP BY `EMP`.`JOB`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleJoinsWithRelationSubquery() {
    String ppl =
        """
        source=EMP
        | head 10
        | inner join left = l right = r ON l.DEPTNO = r.DEPTNO
          [
            source = DEPT
            | where DEPTNO > 10 and LOC = 'CHICAGO'
          ]
        | left join left = l right = r ON l.JOB = r.JOB
          [
            source = BONUS
            | where JOB = 'SALESMAN'
          ]
        | join type=left overwrite=true SAL
          [
            source = SALGRADE
            | where LOSAL <= 1500
            | sort - GRADE
            | rename HISAL as SAL
          ]
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], COMM=[$6],"
            + " DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10], r.ENAME=[$11], r.JOB=[$12],"
            + " r.SAL=[$13], r.COMM=[$14], GRADE=[$15], LOSAL=[$16], SAL=[$17])\n"
            + "  LogicalJoin(condition=[=($5, $17)], joinType=[left])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10],"
            + " r.ENAME=[$11], r.JOB=[$12], r.SAL=[$13], r.COMM=[$14])\n"
            + "      LogicalJoin(condition=[=($2, $12)], joinType=[left])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "          LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "            LogicalSort(fetch=[10])\n"
            + "              LogicalTableScan(table=[[scott, EMP]])\n"
            + "            LogicalFilter(condition=[AND(>($0, 10), =($2, 'CHICAGO':VARCHAR))])\n"
            + "              LogicalTableScan(table=[[scott, DEPT]])\n"
            + "        LogicalFilter(condition=[=($1, 'SALESMAN':VARCHAR)])\n"
            + "          LogicalTableScan(table=[[scott, BONUS]])\n"
            + "    LogicalProject(GRADE=[$0], LOSAL=[$1], SAL=[$2])\n"
            + "      LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "        LogicalFilter(condition=[<=($1, 1500)])\n"
            + "          LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 5);

    String expectedSparkSql =
        "SELECT `t3`.`EMPNO`, `t3`.`ENAME`, `t3`.`JOB`, `t3`.`MGR`, `t3`.`HIREDATE`, `t3`.`COMM`,"
            + " `t3`.`DEPTNO`, `t3`.`r.DEPTNO`, `t3`.`DNAME`, `t3`.`LOC`, `t3`.`r.ENAME`,"
            + " `t3`.`r.JOB`, `t3`.`r.SAL`, `t3`.`r.COMM`, `t6`.`GRADE`, `t6`.`LOSAL`,"
            + " `t6`.`SAL`\n"
            + "FROM (SELECT `t1`.`EMPNO`, `t1`.`ENAME`, `t1`.`JOB`, `t1`.`MGR`, `t1`.`HIREDATE`,"
            + " `t1`.`SAL`, `t1`.`COMM`, `t1`.`DEPTNO`, `t1`.`r.DEPTNO`, `t1`.`DNAME`, `t1`.`LOC`,"
            + " `t2`.`ENAME` `r.ENAME`, `t2`.`JOB` `r.JOB`, `t2`.`SAL` `r.SAL`, `t2`.`COMM`"
            + " `r.COMM`\n"
            + "FROM (SELECT `t`.`EMPNO`, `t`.`ENAME`, `t`.`JOB`, `t`.`MGR`, `t`.`HIREDATE`,"
            + " `t`.`SAL`, `t`.`COMM`, `t`.`DEPTNO`, `t0`.`DEPTNO` `r.DEPTNO`, `t0`.`DNAME`,"
            + " `t0`.`LOC`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 10) `t`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `DEPTNO` > 10 AND `LOC` = 'CHICAGO') `t0` ON `t`.`DEPTNO` = `t0`.`DEPTNO`)"
            + " `t1`\n"
            + "LEFT JOIN (SELECT *\n"
            + "FROM `scott`.`BONUS`\n"
            + "WHERE `JOB` = 'SALESMAN') `t2` ON `t1`.`JOB` = `t2`.`JOB`) `t3`\n"
            + "LEFT JOIN (SELECT `GRADE`, `LOSAL`, `HISAL` `SAL`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `LOSAL` <= 1500\n"
            + "ORDER BY `GRADE` DESC) `t6` ON `t3`.`SAL` = `t6`.`SAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleJoinsWithRelationSubqueryWithAlias() {
    String ppl =
        """
        source=EMP as t1
        | head 10
        | inner join ON t1.DEPTNO = t2.DEPTNO
          [
            source = DEPT as t2
            | where DEPTNO > 10 and LOC = 'CHICAGO'
          ]
        | left join ON t1.JOB = t3.JOB
          [
            source = BONUS as t3
            | where JOB = 'SALESMAN'
          ]
        | join type=left overwrite=true SAL
          [
            source = SALGRADE as t4
            | where LOSAL <= 1500
            | sort - GRADE
            | rename HISAL as SAL
          ]
        """;

    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], COMM=[$6],"
            + " DEPTNO=[$7], DEPT.DEPTNO=[$8], DNAME=[$9], LOC=[$10], BONUS.ENAME=[$11],"
            + " BONUS.JOB=[$12], BONUS.SAL=[$13], BONUS.COMM=[$14], GRADE=[$15], LOSAL=[$16],"
            + " SAL=[$17])\n"
            + "  LogicalJoin(condition=[=($5, $17)], joinType=[left])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPT.DEPTNO=[$8], DNAME=[$9], LOC=[$10],"
            + " BONUS.ENAME=[$11], BONUS.JOB=[$12], BONUS.SAL=[$13], BONUS.COMM=[$14])\n"
            + "      LogicalJoin(condition=[=($2, $12)], joinType=[left])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], DEPT.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "          LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "            LogicalSort(fetch=[10])\n"
            + "              LogicalTableScan(table=[[scott, EMP]])\n"
            + "            LogicalFilter(condition=[AND(>($0, 10), =($2, 'CHICAGO':VARCHAR))])\n"
            + "              LogicalTableScan(table=[[scott, DEPT]])\n"
            + "        LogicalFilter(condition=[=($1, 'SALESMAN':VARCHAR)])\n"
            + "          LogicalTableScan(table=[[scott, BONUS]])\n"
            + "    LogicalProject(GRADE=[$0], LOSAL=[$1], SAL=[$2])\n"
            + "      LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "        LogicalFilter(condition=[<=($1, 1500)])\n"
            + "          LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);

    verifyResultCount(root, 5);

    String expectedSparkSql =
        "SELECT `t3`.`EMPNO`, `t3`.`ENAME`, `t3`.`JOB`, `t3`.`MGR`, `t3`.`HIREDATE`, `t3`.`COMM`,"
            + " `t3`.`DEPTNO`, `t3`.`DEPT.DEPTNO`, `t3`.`DNAME`, `t3`.`LOC`, `t3`.`BONUS.ENAME`,"
            + " `t3`.`BONUS.JOB`, `t3`.`BONUS.SAL`, `t3`.`BONUS.COMM`, `t6`.`GRADE`, `t6`.`LOSAL`,"
            + " `t6`.`SAL`\n"
            + "FROM (SELECT `t1`.`EMPNO`, `t1`.`ENAME`, `t1`.`JOB`, `t1`.`MGR`, `t1`.`HIREDATE`,"
            + " `t1`.`SAL`, `t1`.`COMM`, `t1`.`DEPTNO`, `t1`.`DEPT.DEPTNO`, `t1`.`DNAME`,"
            + " `t1`.`LOC`, `t2`.`ENAME` `BONUS.ENAME`, `t2`.`JOB` `BONUS.JOB`, `t2`.`SAL`"
            + " `BONUS.SAL`, `t2`.`COMM` `BONUS.COMM`\n"
            + "FROM (SELECT `t`.`EMPNO`, `t`.`ENAME`, `t`.`JOB`, `t`.`MGR`, `t`.`HIREDATE`,"
            + " `t`.`SAL`, `t`.`COMM`, `t`.`DEPTNO`, `t0`.`DEPTNO` `DEPT.DEPTNO`, `t0`.`DNAME`,"
            + " `t0`.`LOC`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 10) `t`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `DEPTNO` > 10 AND `LOC` = 'CHICAGO') `t0` ON `t`.`DEPTNO` = `t0`.`DEPTNO`)"
            + " `t1`\n"
            + "LEFT JOIN (SELECT *\n"
            + "FROM `scott`.`BONUS`\n"
            + "WHERE `JOB` = 'SALESMAN') `t2` ON `t1`.`JOB` = `t2`.`JOB`) `t3`\n"
            + "LEFT JOIN (SELECT `GRADE`, `LOSAL`, `HISAL` `SAL`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `LOSAL` <= 1500\n"
            + "ORDER BY `GRADE` DESC) `t6` ON `t3`.`SAL` = `t6`.`SAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultipleJoinsWithRelationSubqueryWithAlias2() {
    String ppl =
        """
        source=EMP as t1
        | head 10
        | inner join left = l right = r ON t1.DEPTNO = t2.DEPTNO
          [
            source = DEPT as t2
            | where DEPTNO > 10 and LOC = 'CHICAGO'
          ]
        | left join left = l right = r ON t1.JOB = t3.JOB
          [
            source = BONUS as t3
            | where JOB = 'SALESMAN'
          ]
        | join type=left overwrite=true SAL
          [
            source = SALGRADE as t4
            | where LOSAL <= 1500
            | sort - GRADE
            | rename HISAL as SAL
          ]
        """;
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], COMM=[$6],"
            + " DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10], r.ENAME=[$11], r.JOB=[$12],"
            + " r.SAL=[$13], r.COMM=[$14], GRADE=[$15], LOSAL=[$16], SAL=[$17])\n"
            + "  LogicalJoin(condition=[=($5, $17)], joinType=[left])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10],"
            + " r.ENAME=[$11], r.JOB=[$12], r.SAL=[$13], r.COMM=[$14])\n"
            + "      LogicalJoin(condition=[=($2, $12)], joinType=[left])\n"
            + "        LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "          LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "            LogicalSort(fetch=[10])\n"
            + "              LogicalTableScan(table=[[scott, EMP]])\n"
            + "            LogicalFilter(condition=[AND(>($0, 10), =($2, 'CHICAGO':VARCHAR))])\n"
            + "              LogicalTableScan(table=[[scott, DEPT]])\n"
            + "        LogicalFilter(condition=[=($1, 'SALESMAN':VARCHAR)])\n"
            + "          LogicalTableScan(table=[[scott, BONUS]])\n"
            + "    LogicalProject(GRADE=[$0], LOSAL=[$1], SAL=[$2])\n"
            + "      LogicalSort(sort0=[$0], dir0=[DESC-nulls-last])\n"
            + "        LogicalFilter(condition=[<=($1, 1500)])\n"
            + "          LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);

    verifyResultCount(root, 5);

    String expectedSparkSql =
        "SELECT `t3`.`EMPNO`, `t3`.`ENAME`, `t3`.`JOB`, `t3`.`MGR`, `t3`.`HIREDATE`, `t3`.`COMM`,"
            + " `t3`.`DEPTNO`, `t3`.`r.DEPTNO`, `t3`.`DNAME`, `t3`.`LOC`, `t3`.`r.ENAME`,"
            + " `t3`.`r.JOB`, `t3`.`r.SAL`, `t3`.`r.COMM`, `t6`.`GRADE`, `t6`.`LOSAL`, `t6`.`SAL`\n"
            + "FROM (SELECT `t1`.`EMPNO`, `t1`.`ENAME`, `t1`.`JOB`, `t1`.`MGR`, `t1`.`HIREDATE`,"
            + " `t1`.`SAL`, `t1`.`COMM`, `t1`.`DEPTNO`, `t1`.`r.DEPTNO`, `t1`.`DNAME`, `t1`.`LOC`,"
            + " `t2`.`ENAME` `r.ENAME`, `t2`.`JOB` `r.JOB`, `t2`.`SAL` `r.SAL`, `t2`.`COMM`"
            + " `r.COMM`\n"
            + "FROM (SELECT `t`.`EMPNO`, `t`.`ENAME`, `t`.`JOB`, `t`.`MGR`, `t`.`HIREDATE`,"
            + " `t`.`SAL`, `t`.`COMM`, `t`.`DEPTNO`, `t0`.`DEPTNO` `r.DEPTNO`, `t0`.`DNAME`,"
            + " `t0`.`LOC`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "LIMIT 10) `t`\n"
            + "INNER JOIN (SELECT *\n"
            + "FROM `scott`.`DEPT`\n"
            + "WHERE `DEPTNO` > 10 AND `LOC` = 'CHICAGO') `t0` ON `t`.`DEPTNO` = `t0`.`DEPTNO`)"
            + " `t1`\n"
            + "LEFT JOIN (SELECT *\n"
            + "FROM `scott`.`BONUS`\n"
            + "WHERE `JOB` = 'SALESMAN') `t2` ON `t1`.`JOB` = `t2`.`JOB`) `t3`\n"
            + "LEFT JOIN (SELECT `GRADE`, `LOSAL`, `HISAL` `SAL`\n"
            + "FROM `scott`.`SALGRADE`\n"
            + "WHERE `LOSAL` <= 1500\n"
            + "ORDER BY `GRADE` DESC) `t6` ON `t3`.`SAL` = `t6`.`SAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithFieldList() {
    String ppl = "source=EMP | join DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `DEPT`.`DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSplJoinWithJoinArguments() {
    String ppl = "source=EMP | join type=inner overwrite=false DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);
    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithFieldListAndJoinArguments2() {
    String ppl = "source=EMP | join type=left overwrite=false DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSemiJoinWithFieldList() {
    String ppl = "source=EMP | join type=semi overwrite=true DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalJoin(condition=[=($7, $8)], joinType=[semi])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT SEMI JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithoutFieldList() {
    String ppl = "source=EMP | join DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `DEPT`.`DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithoutFieldListSelfJoin() {
    String ppl = "source=EMP | join EMP";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$8], ENAME=[$9], JOB=[$10], MGR=[$11], HIREDATE=[$12], SAL=[$13],"
            + " COMM=[$14], DEPTNO=[$15])\n"
            + "  LogicalJoin(condition=[AND(=($0, $8), =($1, $9), =($2, $10), =($3, $11), =($4,"
            + " $12), =($5, $13), =($6, $14), =($7, $15))], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 4);

    String expectedSparkSql =
        "SELECT `EMP0`.`EMPNO`, `EMP0`.`ENAME`, `EMP0`.`JOB`, `EMP0`.`MGR`, `EMP0`.`HIREDATE`,"
            + " `EMP0`.`SAL`, `EMP0`.`COMM`, `EMP0`.`DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`EMP` `EMP0` ON `EMP`.`EMPNO` = `EMP0`.`EMPNO` AND `EMP`.`ENAME`"
            + " = `EMP0`.`ENAME` AND (`EMP`.`JOB` = `EMP0`.`JOB` AND `EMP`.`MGR` = `EMP0`.`MGR`)"
            + " AND (`EMP`.`HIREDATE` = `EMP0`.`HIREDATE` AND `EMP`.`SAL` = `EMP0`.`SAL` AND"
            + " (`EMP`.`COMM` = `EMP0`.`COMM` AND `EMP`.`DEPTNO` = `EMP0`.`DEPTNO`))";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithMultiplePredicatesWithWhere() {
    String ppl =
        "source=EMP | join left = l right = r where l.DEPTNO = r.DEPTNO AND l.DEPTNO > 10 AND"
            + " EMP.SAL < 3000 DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[AND(=($7, $8), >($7, 10), <($5, 3000))],"
            + " joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 9);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `r.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO` AND `EMP`.`DEPTNO` >"
            + " 10 AND `EMP`.`SAL` < 3000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithFieldListSelfJoinOverrideIsFalse() {
    String ppl = "source=EMP | join type=outer overwrite=false EMPNO ENAME JOB, MGR EMP";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalJoin(condition=[AND(=($0, $8), =($1, $9), =($2, $10), =($3, $11))],"
            + " joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`EMP` `EMP0` ON `EMP`.`EMPNO` = `EMP0`.`EMPNO` AND `EMP`.`ENAME` ="
            + " `EMP0`.`ENAME` AND `EMP`.`JOB` = `EMP0`.`JOB` AND `EMP`.`MGR` = `EMP0`.`MGR`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithFieldListSelfJoinOverrideIsTrue() {
    String ppl = "source=EMP | join type=outer overwrite=true EMPNO ENAME JOB, MGR EMP";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$8], ENAME=[$9], JOB=[$10], MGR=[$11], HIREDATE=[$12], SAL=[$13],"
            + " COMM=[$14], DEPTNO=[$15])\n"
            + "  LogicalJoin(condition=[AND(=($0, $8), =($1, $9), =($2, $10), =($3, $11))],"
            + " joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP0`.`EMPNO`, `EMP0`.`ENAME`, `EMP0`.`JOB`, `EMP0`.`MGR`, `EMP0`.`HIREDATE`,"
            + " `EMP0`.`SAL`, `EMP0`.`COMM`, `EMP0`.`DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`EMP` `EMP0` ON `EMP`.`EMPNO` = `EMP0`.`EMPNO` AND `EMP`.`ENAME` ="
            + " `EMP0`.`ENAME` AND `EMP`.`JOB` = `EMP0`.`JOB` AND `EMP`.`MGR` = `EMP0`.`MGR`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithFieldListSelfJoin() {
    String ppl = "source=EMP | join type=outer EMPNO ENAME JOB, MGR EMP";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$8], ENAME=[$9], JOB=[$10], MGR=[$11], HIREDATE=[$12], SAL=[$13],"
            + " COMM=[$14], DEPTNO=[$15])\n"
            + "  LogicalJoin(condition=[AND(=($0, $8), =($1, $9), =($2, $10), =($3, $11))],"
            + " joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP0`.`EMPNO`, `EMP0`.`ENAME`, `EMP0`.`JOB`, `EMP0`.`MGR`, `EMP0`.`HIREDATE`,"
            + " `EMP0`.`SAL`, `EMP0`.`COMM`, `EMP0`.`DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`EMP` `EMP0` ON `EMP`.`EMPNO` = `EMP0`.`EMPNO` AND `EMP`.`ENAME` ="
            + " `EMP0`.`ENAME` AND `EMP`.`JOB` = `EMP0`.`JOB` AND `EMP`.`MGR` = `EMP0`.`MGR`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testDisableHighCostJoinTypes() {
    String ppl1 = "source=EMP as e | full join on e.DEPTNO = d.DEPTNO DEPT as d";
    String ppl2 = "source=EMP | join type=full overwrite=false EMPNO ENAME JOB, MGR EMP";
    String err =
        "Join type FULL is performance sensitive. Set plugins.calcite.all_join_types.allowed to"
            + " true to enable it.";

    // disable high cost join types
    doReturn(false).when(settings).getSettingValue(Settings.Key.CALCITE_SUPPORT_ALL_JOIN_TYPES);
    Throwable t = Assert.assertThrows(SemanticCheckException.class, () -> getRelNode(ppl1));
    verifyErrorMessageContains(t, err);
    t = Assert.assertThrows(SemanticCheckException.class, () -> getRelNode(ppl2));
    verifyErrorMessageContains(t, err);
    // enable high cost types
    doReturn(true).when(settings).getSettingValue(Settings.Key.CALCITE_SUPPORT_ALL_JOIN_TYPES);
    getRelNode(ppl1);
    getRelNode(ppl2);
  }

  @Test
  public void testJoinWithFieldListMaxGreaterThanZero() {
    String ppl = "source=EMP | join type=outer max=1 DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
            + "      LogicalFilter(condition=[<=($3, 1)])\n"
            + "        LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $0)])\n"
            + "          LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `t1`.`DEPTNO`, `t1`.`DNAME`, `t1`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN (SELECT `DEPTNO`, `DNAME`, `LOC`\n"
            + "FROM (SELECT `DEPTNO`, `DNAME`, `LOC`, ROW_NUMBER() OVER (PARTITION BY `DEPTNO`)"
            + " `_row_number_dedup_`\n"
            + "FROM `scott`.`DEPT`) `t`\n"
            + "WHERE `_row_number_dedup_` <= 1) `t1` ON `EMP`.`DEPTNO` = `t1`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithCriteriaMaxGreaterThanZero() {
    String ppl = "source=EMP | outer join max=1 left=l right=r on l.DEPTNO=r.DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
            + "      LogicalFilter(condition=[<=($3, 1)])\n"
            + "        LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $0)])\n"
            + "          LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `t1`.`DEPTNO` `r.DEPTNO`, `t1`.`DNAME`,"
            + " `t1`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN (SELECT `DEPTNO`, `DNAME`, `LOC`\n"
            + "FROM (SELECT `DEPTNO`, `DNAME`, `LOC`, ROW_NUMBER() OVER (PARTITION BY `DEPTNO`)"
            + " `_row_number_dedup_`\n"
            + "FROM `scott`.`DEPT`) `t`\n"
            + "WHERE `_row_number_dedup_` <= 1) `t1` ON `EMP`.`DEPTNO` = `t1`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWithMaxEqualsZero() {
    String ppl = "source=EMP | join type=outer max=0 DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `DEPT`.`DEPTNO`, `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinWhenLegacyNotPreferred() {
    doReturn(false).when(settings).getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED);
    String ppl = "source=EMP | join type=outer DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2])\n"
            + "      LogicalFilter(condition=[<=($3, 1)])\n"
            + "        LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2],"
            + " _row_number_dedup_=[ROW_NUMBER() OVER (PARTITION BY $0)])\n"
            + "          LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `t1`.`DEPTNO`, `t1`.`DNAME`, `t1`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN (SELECT `DEPTNO`, `DNAME`, `LOC`\n"
            + "FROM (SELECT `DEPTNO`, `DNAME`, `LOC`, ROW_NUMBER() OVER (PARTITION BY `DEPTNO`)"
            + " `_row_number_dedup_`\n"
            + "FROM `scott`.`DEPT`) `t`\n"
            + "WHERE `_row_number_dedup_` <= 1) `t1` ON `EMP`.`DEPTNO` = `t1`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testJoinSubsearchMaxOut() {
    String ppl1 = "source=EMP | join type=inner max=0 DEPTNO DEPT";
    RelNode root1 = getRelNode(ppl1);
    verifyResultCount(root1, 14); // no limit
    String ppl2 = "source=EMP | inner join left=l right=r on l.DEPTNO=r.DEPTNO DEPT";
    RelNode root2 = getRelNode(ppl2);
    verifyResultCount(root2, 14); // no limit for sql-like syntax

    doReturn(1).when(settings).getSettingValue(Settings.Key.PPL_JOIN_SUBSEARCH_MAXOUT);
    root1 = getRelNode(ppl1);
    verifyResultCount(root1, 3); // set maxout of subsesarch to 1
    root2 = getRelNode(ppl2);
    verifyResultCount(root2, 3); // set maxout to 1 for sql-like syntax
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalSystemLimit(sort0=[$0], dir0=[ASC], fetch=[1],"
            + " type=[JOIN_SUBSEARCH_MAXOUT])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root1, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `t`.`DEPTNO`, `t`.`DNAME`, `t`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "INNER JOIN (SELECT `DEPTNO`, `DNAME`, `LOC`\n"
            + "FROM `scott`.`DEPT`\n"
            + "ORDER BY `DEPTNO` NULLS LAST\n"
            + "LIMIT 1) `t` ON `EMP`.`DEPTNO` = `t`.`DEPTNO`";
    verifyPPLToSparkSQL(root1, expectedSparkSql);
  }

  @Test
  public void testJoinWithMaxLessThanZero() {
    String ppl = "source=EMP | join type=outer max=-1 DEPTNO DEPT";
    Throwable t = Assert.assertThrows(SemanticCheckException.class, () -> getRelNode(ppl));
    verifyErrorMessageContains(t, "max option must be a positive integer");
  }

  @Test
  public void testSqlLikeJoinWithSpecificJoinType() {
    String ppl = "source=EMP | join type=left left=l right=r on l.DEPTNO=r.DEPTNO DEPT";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7], r.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[left])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
    verifyResultCount(root, 14);

    String expectedSparkSql =
        "SELECT `EMP`.`EMPNO`, `EMP`.`ENAME`, `EMP`.`JOB`, `EMP`.`MGR`, `EMP`.`HIREDATE`,"
            + " `EMP`.`SAL`, `EMP`.`COMM`, `EMP`.`DEPTNO`, `DEPT`.`DEPTNO` `r.DEPTNO`,"
            + " `DEPT`.`DNAME`, `DEPT`.`LOC`\n"
            + "FROM `scott`.`EMP`\n"
            + "LEFT JOIN `scott`.`DEPT` ON `EMP`.`DEPTNO` = `DEPT`.`DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
