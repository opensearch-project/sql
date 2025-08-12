/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Test suite for PPL fields and table command functionality with Calcite engine. Tests both
 * comma-delimited and space-delimited field selection syntax. Since fields and table commands are
 * functionally equivalent in Calcite, this test covers both commands.
 */
public class CalcitePPLFieldsTest extends CalcitePPLAbstractTest {

  public CalcitePPLFieldsTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  private void testQuery(String query, String expectedLogical, String expectedSparkSql) {
    RelNode root = getRelNode(query);
    verifyLogical(root, expectedLogical);
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testBasicSelection() {
    testQuery(
        "source=EMP | fields EMPNO, ENAME",
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testTableCommandEquivalence() {
    testQuery(
        "source=EMP | table EMPNO, ENAME",
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSpaceDelimitedSyntax() {
    testQuery(
        "source=EMP | fields EMPNO ENAME JOB",
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `ENAME`, `JOB`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testWildcardMixedFields() {
    testQuery(
        "source=EMP | fields JOB, EMP*, *NO",
        "LogicalProject(JOB=[$2], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `JOB`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testWildcardPatternMatching() {
    testQuery(
        "source=EMP | fields EMP*, *NO, *A*",
        "LogicalProject(EMPNO=[$0], DEPTNO=[$7], ENAME=[$1], HIREDATE=[$4], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `DEPTNO`, `ENAME`, `HIREDATE`, `SAL`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testMultipleWildcardCombinations() {
    testQuery(
        "source=EMP | table *E*, *A*, *O",
        "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], DEPTNO=[$7], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `ENAME`, `HIREDATE`, `DEPTNO`, `SAL`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testExplicitFieldExclusion() {
    testQuery(
        "source=EMP | fields - EMPNO, ENAME, JOB",
        "LogicalProject(MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testWildcardFieldExclusion() {
    testQuery(
        "source=EMP | fields - *NO",
        "LogicalProject(ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testCustomFieldOrdering() {
    testQuery(
        "source=EMP | fields SAL, EMPNO, ENAME",
        "LogicalProject(SAL=[$5], EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `SAL`, `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testExplicitInclusionSyntax() {
    testQuery(
        "source=EMP | fields + EMPNO, JOB, SAL",
        "LogicalProject(EMPNO=[$0], JOB=[$2], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `JOB`, `SAL`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testDuplicateFieldDeduplication() {
    testQuery(
        "source=EMP | fields EMPNO, ENAME, EMPNO",
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSelectAllWithWildcard() {
    testQuery(
        "source=EMP | fields *, EMPNO, EMP*",
        "LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT *\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testMixedCommandPipeline() {
    testQuery(
        "source=EMP | fields EMP*, SAL | table *, EMPNO | fields *",
        "LogicalProject(EMPNO=[$0], SAL=[$5])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `SAL`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testSequentialFieldExclusion() {
    testQuery(
        "source=EMP | fields * | fields - *NO, JOB",
        "LogicalProject(ENAME=[$1], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `ENAME`, `MGR`, `HIREDATE`, `SAL`, `COMM`\n" + "FROM `scott`.`EMP`");
  }

  @Test
  public void testCommaSpaceMixedDelimiters() {
    testQuery(
        "source=EMP | table EMPNO, ENAME JOB, SAL",
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT `EMPNO`, `ENAME`, `JOB`, `SAL`\n" + "FROM `scott`.`EMP`");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidWildcardPattern() {
    getRelNode("source=EMP | fields XYZ*");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExcludeAllFieldsValidation() {
    getRelNode("source=EMP | fields - *");
  }

  @Test
  public void testIncludeAllFieldsVariations() {
    testQuery(
        "source=EMP | fields + *",
        "LogicalTableScan(table=[[scott, EMP]])\n",
        "SELECT *\n" + "FROM `scott`.`EMP`");
  }
}
