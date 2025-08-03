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
  public void testFieldsBasic() {
    String ppl = "source=EMP | fields EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldsSpaceDelimited() {
    String ppl = "source=EMP | fields EMPNO ENAME JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldsWithPrefixWildcard() {
    String ppl = "source=EMP | fields EMP*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldsWithSuffixWildcard() {
    String ppl = "source=EMP | fields *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldsWithMixedWildcards() {
    String ppl = "source=EMP | fields JOB, EMP*, *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `JOB`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTableWithPrefixWildcard() {
    String ppl = "source=EMP | table EMP*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testTableWithMixedWildcards() {
    String ppl = "source=EMP | table JOB, EMP*, *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `JOB`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldsAndTableEquivalence() {
    String fieldsQuery = "source=EMP | fields JOB EMP* *NO";
    String tableQuery = "source=EMP | table JOB EMP* *NO";

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
  public void testFieldsMinusSpaceDelimited() {
    String ppl = "source=EMP | fields - EMPNO ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests wildcard patterns that match fields containing specific characters. */
  @Test
  public void testFieldsWithContainsWildcard() {
    String ppl = "source=EMP | fields *A*";
    RelNode root = getRelNode(ppl);
    // Matches ENAME, HIREDATE, and SAL
    String expectedLogical =
        "LogicalProject(ENAME=[$1], HIREDATE=[$4], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `HIREDATE`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests complex wildcard patterns with multiple asterisks. */
  @Test
  public void testFieldsWithComplexWildcardPattern() {
    String ppl = "source=EMP | fields *E*O";
    RelNode root = getRelNode(ppl);
    // Matches fields containing 'E' and ending with 'O'
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests table command with multiple different wildcard patterns combined. */
  @Test
  public void testTableWithComplexMixedWildcards() {
    String ppl = "source=EMP | table *E*, *A*, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], DEPTNO=[$7], SAL=[$5], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `HIREDATE`, `DEPTNO`, `SAL`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests field exclusion using wildcard patterns with minus operator. */
  @Test
  public void testFieldsMinusWithWildcards() {
    String ppl = "source=EMP | fields - *NO";
    RelNode root = getRelNode(ppl);
    // Excludes EMPNO and DEPTNO
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests excluding multiple specific fields from table output. */
  @Test
  public void testTableMinusMultipleFields() {
    String ppl = "source=EMP | table - EMPNO, ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests complex mixed delimiter syntax combining commas and spaces. */
  @Test
  public void testTableMixedDelimitersComplex() {
    String ppl = "source=EMP | table EMPNO, ENAME JOB, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests comma-delimited field syntax with wildcard patterns. */
  @Test
  public void testFieldsCommaDelimitedWithWildcards() {
    String ppl = "source=EMP | fields EMPNO,*A*,JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], SAL=[$5], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `HIREDATE`, `SAL`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests that field ordering is preserved as specified in the query. */
  @Test
  public void testFieldsOrdering() {
    String ppl = "source=EMP | fields SAL, EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SAL=[$5], EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `SAL`, `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests field ordering when wildcards are mixed with explicit fields. */
  @Test
  public void testTableFieldOrderingWithWildcards() {
    String ppl = "source=EMP | table SAL, EMP*, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(SAL=[$5], EMPNO=[$0], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `SAL`, `EMPNO`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests explicit field inclusion using plus prefix syntax. */
  @Test
  public void testFieldsExplicitIncludeWithPlusPrefix() {
    String ppl = "source=EMP | fields + EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests explicit inclusion of multiple fields with plus prefix. */
  @Test
  public void testFieldsExplicitIncludeMultiple() {
    String ppl = "source=EMP | fields + EMPNO, JOB, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], JOB=[$2], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `JOB`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests automatic deduplication when same field is specified multiple times. */
  @Test
  public void testFieldsWithDuplicateFields() {
    String ppl = "source=EMP | fields EMPNO, ENAME, EMPNO";
    RelNode root = getRelNode(ppl);
    // Deduplicates repeated EMPNO
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests deduplication when wildcard and explicit field reference same column. */
  @Test
  public void testTableWithDuplicateWildcardMatches() {
    String ppl = "source=EMP | table EMP*, EMPNO";
    RelNode root = getRelNode(ppl);
    // EMP* matches EMPNO, deduplicates explicit EMPNO
    String expectedLogical =
        "LogicalProject(EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests performance with selection of many fields simultaneously. */
  @Test
  public void testFieldsWithManyFields() {
    String ppl = "source=EMP | fields EMPNO, ENAME, JOB, MGR, HIREDATE, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests complex combinations of multiple overlapping wildcard patterns. */
  @Test
  public void testTableWithComplexWildcardCombinations() {
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

  /** Tests error handling when wildcard pattern matches no existing fields. */
  @Test(expected = IllegalArgumentException.class)
  public void testFieldsWithNoMatchingWildcard() {
    String ppl = "source=EMP | fields XYZ*";
    // No fields match XYZ* - should throw IllegalArgumentException
    getRelNode(ppl);
  }

  /** Tests error handling for table command with non-matching wildcard. */
  @Test(expected = IllegalArgumentException.class)
  public void testTableWithNoMatchingWildcard() {
    String ppl = "source=EMP | table *XYZ";
    // No fields match *XYZ - should throw IllegalArgumentException
    getRelNode(ppl);
  }
}
