/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Tests for QualifiedNameResolver behavior in actual query planning scenarios. These tests verify
 * that qualified name resolution works correctly during PPL query planning and produces the
 * expected logical plans.
 */
public class CalcitePPLQualifiedNameResolutionTest extends CalcitePPLAbstractTest {

  public CalcitePPLQualifiedNameResolutionTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testSimpleFieldReference() {
    String ppl = "source=EMP | where DEPTNO = 20 | fields EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testQualifiedFieldReference() {
    String ppl = "source=EMP as e | where e.DEPTNO = 20 | fields e.EMPNO, e.ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testJoinWithQualifiedNames() {
    String ppl =
        "source=EMP as e | join on e.DEPTNO = d.DEPTNO [ source=DEPT as d ] | fields e.EMPNO,"
            + " e.ENAME, d.DNAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], DNAME=[$9])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testJoinWithMixedQualifiedAndUnqualifiedNames() {
    String ppl =
        "source=EMP as e | join on e.DEPTNO = d.DEPTNO [ source=DEPT as d ] | fields e.EMPNO,"
            + " e.ENAME, d.DNAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], DNAME=[$9])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testJoinWithDuplicateFieldNames() {
    String ppl =
        "source=EMP as e | join on e.DEPTNO = d.DEPTNO [ source=DEPT as d ] | fields e.DEPTNO,"
            + " d.DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$7], d.DEPTNO=[$8])\n"
            + "  LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testComplexJoinCondition() {
    String ppl =
        "source=EMP as e | join on e.DEPTNO = d.DEPTNO AND e.SAL > 1000 [ source=DEPT as d ] |"
            + " fields e.EMPNO, d.DNAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DNAME=[$9])\n"
            + "  LogicalJoin(condition=[AND(=($7, $8), >($5, 1000))], joinType=[inner])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testRenamedFieldAccess() {
    String ppl =
        "source=EMP | rename DEPTNO as DEPT_ID | where DEPT_ID = 20 | fields EMPNO, DEPT_ID";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DEPT_ID=[$7])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPT_ID=[$7])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMultipleTableAliases() {
    String ppl =
        "source=EMP as emp | join on emp.DEPTNO = dept.DEPTNO [ source=DEPT as dept ] | where"
            + " emp.SAL > 2000 | fields emp.EMPNO, dept.DNAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DNAME=[$9])\n"
            + "  LogicalFilter(condition=[>($5, 2000)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], dept.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "      LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "        LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testFieldAccessWithoutAlias() {
    String ppl = "source=EMP | where DEPTNO = 20 AND SAL > 1000 | fields EMPNO, ENAME, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "  LogicalFilter(condition=[AND(=($7, 20), >($5, 1000))])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testComplexExpressionWithQualifiedNames() {
    String ppl =
        "source=EMP as e | eval bonus = e.SAL * 0.1 | where e.DEPTNO = 20 | fields e.EMPNO, e.SAL,"
            + " bonus";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], SAL=[$5], bonus=[$8])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], bonus=[*($5, 0.1:DECIMAL(2, 1))])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testQualifiedNameInSortAndLimit() {
    String ppl = "source=EMP as e | sort e.SAL desc | head 5 | fields e.EMPNO, e.ENAME, e.SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "  LogicalSort(sort0=[$5], dir0=[DESC-nulls-last], fetch=[5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testQualifiedNameInAggregation() {
    String ppl =
        "source=EMP as e | stats avg(e.SAL) as avg_sal by e.DEPTNO | fields e.DEPTNO, avg_sal";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalAggregate(group=[{0}], avg_sal=[AVG($1)])\n"
            + "  LogicalProject(e.DEPTNO=[$7], SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMultipleJoinsWithQualifiedNames() {
    String ppl =
        "source=EMP as e | join on e.DEPTNO = d.DEPTNO [ source=DEPT as d ] | join on e.SAL >"
            + " s.LOSAL AND e.SAL < s.HISAL [ source=SALGRADE as s ] | fields e.EMPNO, d.DNAME,"
            + " s.GRADE";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DNAME=[$9], GRADE=[$11])\n"
            + "  LogicalJoin(condition=[AND(>($5, $12), <($5, $13))], joinType=[inner])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], d.DEPTNO=[$8], DNAME=[$9], LOC=[$10])\n"
            + "      LogicalJoin(condition=[=($7, $8)], joinType=[inner])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n"
            + "        LogicalTableScan(table=[[scott, DEPT]])\n"
            + "    LogicalTableScan(table=[[scott, SALGRADE]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testCorrelatedSubqueryWithQualifiedNames() {
    String ppl =
        "source=DEPT | where DEPTNO in [ source=EMP | where DEPTNO = DEPT.DEPTNO | fields DEPTNO ]"
            + " | fields DNAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DNAME=[$1])\n"
            + "  LogicalFilter(condition=[IN($0, {\n"
            + "LogicalProject(DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[=($7, $cor0.DEPTNO)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "    LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testFieldContainsDots() {
    String ppl =
        "source=DEPT | eval department.number = DEPTNO, DEPT.number = DEPTNO | where"
            + " department.number in [ source=EMP | where DEPTNO = department.number | fields"
            + " DEPTNO ] | fields DNAME, DEPT.number";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DNAME=[$1], DEPT.number=[$4])\n"
            + "  LogicalFilter(condition=[IN($3, {\n"
            + "LogicalProject(DEPTNO=[$7])\n"
            + "  LogicalFilter(condition=[=($7, $cor0.department.number)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "})], variablesSet=[[$cor0]])\n"
            + "    LogicalProject(DEPTNO=[$0], DNAME=[$1], LOC=[$2], department.number=[$0],"
            + " DEPT.number=[$0])\n"
            + "      LogicalTableScan(table=[[scott, DEPT]])\n";
    verifyLogical(root, expectedLogical);
  }
}
