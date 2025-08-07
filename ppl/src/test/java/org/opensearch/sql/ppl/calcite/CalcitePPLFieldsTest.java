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

  @Test
  public void testFieldsAndTableBasicSelection() {
    String fieldsQuery = "source=EMP | fields EMPNO, ENAME";
    String tableQuery = "source=EMP | table EMPNO, ENAME";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testSpaceDelimitedSyntax() {
    String fieldsQuery = "source=EMP | fields EMPNO ENAME JOB";
    String tableQuery = "source=EMP | table EMPNO ENAME JOB";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testFieldsAndTableEquivalence() {
    String fieldsQuery = "source=EMP | fields JOB, EMP*, *NO";
    String tableQuery = "source=EMP | table JOB, EMP*, *NO";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(JOB=[$2], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql = "SELECT `JOB`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testWildcardPatternMatching() {
    String fieldsQuery = "source=EMP | fields EMP*, *NO, *A*";
    String tableQuery = "source=EMP | table EMP*, *NO, *A*";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DEPTNO=[$7], ENAME=[$1], HIREDATE=[$4], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `DEPTNO`, `ENAME`, `HIREDATE`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testMultipleWildcardCombinations() {
    String ppl = "source=EMP | table *E*, *A*, *O";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], DEPTNO=[$7], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `HIREDATE`, `DEPTNO`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testExplicitFieldExclusion() {
    String fieldsQuery = "source=EMP | fields - EMPNO, ENAME, JOB";
    String tableQuery = "source=EMP | table - EMPNO, ENAME, JOB";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql =
        "SELECT `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testWildcardFieldExclusion() {
    String fieldsQuery = "source=EMP | fields - *NO";
    String tableQuery = "source=EMP | table - *NO";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testCustomFieldOrdering() {
    String fieldsQuery = "source=EMP | fields SAL, EMPNO, ENAME";
    String tableQuery = "source=EMP | table SAL, EMPNO, ENAME";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(SAL=[$5], EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql = "SELECT `SAL`, `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testExplicitInclusionSyntax() {
    String fieldsQuery = "source=EMP | fields + EMPNO, JOB, SAL";
    String tableQuery = "source=EMP | table + EMPNO, JOB, SAL";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], JOB=[$2], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `JOB`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testDuplicateFieldDeduplication() {
    String fieldsQuery = "source=EMP | fields EMPNO, ENAME, EMPNO";
    String tableQuery = "source=EMP | table EMPNO, ENAME, EMPNO";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testSelectAllWithWildcard() {
    String fieldsQuery = "source=EMP | fields *, EMPNO, EMP*";
    String tableQuery = "source=EMP | table *, EMPNO, EMP*";

    RelNode fieldsRoot = getRelNode(fieldsQuery);
    RelNode tableRoot = getRelNode(tableQuery);

    String expectedLogical = "LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(fieldsRoot, expectedLogical);
    verifyLogical(tableRoot, expectedLogical);

    String expectedSparkSql = "SELECT *\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(fieldsRoot, expectedSparkSql);
    verifyPPLToSparkSQL(tableRoot, expectedSparkSql);
  }

  @Test
  public void testMixedCommandPipeline() {
    String ppl = "source=EMP | fields EMP*, SAL | table *, EMPNO | fields *";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], SAL=[$5])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testSequentialFieldExclusion() {
    String ppl = "source=EMP | fields * | fields - *NO, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `MGR`, `HIREDATE`, `SAL`, `COMM`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testCommaSpaceMixedDelimiters() {
    String ppl = "source=EMP | table EMPNO, ENAME JOB, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidWildcardPattern() {
    String ppl = "source=EMP | fields XYZ*";
    getRelNode(ppl);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTableInvalidWildcardPattern() {
    String ppl = "source=EMP | table *XYZ";
    getRelNode(ppl);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExcludeAllFieldsValidation() {

    getRelNode("source=EMP | fields - *");
    getRelNode("source=EMP | table - *");
    getRelNode("source=EMP | fields - EMP* ENAME JOB, *");
    getRelNode("source=EMP | table - EMP* ENAME JOB, *");
    getRelNode("source=EMP | fields EMP* ENAME JOB, * | fields - *");
    getRelNode("source=EMP | table EMP* ENAME JOB, * | table - *");
    getRelNode("source=EMP | fields EMP* ENAME JOB, * | fields + * | fields - *");
    getRelNode("source=EMP | fields EMP* ENAME JOB, * | fields - * | fields + *");
  }

  @Test
  public void testIncludeAllFieldsVariations() {
    String expectedLogical = "LogicalTableScan(table=[[scott, EMP]])\n";
    String expectedSparkSql = "SELECT *\n" + "FROM `scott`.`EMP`";

    String[] queries = {
      "source=EMP | fields + *",
      "source=EMP | table + *",
      "source=EMP | fields EMP* ENAME JOB, * | fields + * | fields + *",
      "source=EMP | table EMP* ENAME JOB, * | table + * | table + *"
    };

    for (String ppl : queries) {
      RelNode root = getRelNode(ppl);
      verifyLogical(root, expectedLogical);
      verifyPPLToSparkSQL(root, expectedSparkSql);
    }
  }
}
