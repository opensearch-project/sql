/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Test suite for PPL fields command functionality with Calcite engine. Tests both comma-delimited
 * and space-delimited field selection syntax.
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

  /** Tests space-delimited field selection syntax. */
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

  /** Tests equivalence between space-delimited and comma-delimited field syntax. */
  @Test
  public void testFieldsSpaceDelimitedEquivalentToCommaDelimited() {
    String pplComma = "source=EMP | fields EMPNO, ENAME, JOB";
    String pplSpace = "source=EMP | fields EMPNO ENAME JOB";

    RelNode rootComma = getRelNode(pplComma);
    RelNode rootSpace = getRelNode(pplSpace);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(rootComma, expectedLogical);
    verifyLogical(rootSpace, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(rootComma, expectedSparkSql);
    verifyPPLToSparkSQL(rootSpace, expectedSparkSql);
  }

  @Test
  public void testFieldsSpaceDelimitedWithFilter() {
    String ppl = "source=EMP | where DEPTNO = 20 | fields EMPNO ENAME SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `SAL`\n" + "FROM `scott`.`EMP`\n" + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldsSpaceDelimitedWithSort() {
    String ppl = "source=EMP | sort SAL | fields EMPNO ENAME SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "  LogicalSort(sort0=[$5], dir0=[ASC-nulls-first])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `SAL`\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `SAL`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests field exclusion with space-delimited syntax. */
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

  /** Tests explicit field inclusion with space-delimited syntax. */
  @Test
  public void testFieldsPlusSpaceDelimited() {
    String ppl = "source=EMP | fields + EMPNO ENAME JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testFieldsSpaceDelimitedWithHead() {
    String ppl = "source=EMP | fields EMPNO ENAME SAL | head 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[3])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1], SAL=[$5])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `SAL`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests space-delimited fields in complex pipeline operations. Note: Calcite optimizes sort and
   * limit into single LogicalSort with fetch parameter.
   */
  @Test
  public void testFieldsSpaceDelimitedComplexPipeline() {
    String ppl =
        "source=EMP | where DEPTNO in (10, 20) | sort - SAL | fields EMPNO ENAME DEPTNO SAL | head"
            + " 5";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], DEPTNO=[$7], SAL=[$5])\n"
            + "  LogicalSort(sort0=[$5], dir0=[DESC-nulls-last], fetch=[5])\n"
            + "    LogicalFilter(condition=[SEARCH($7, Sarg[10, 20])])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `DEPTNO`, `SAL`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IN (10, 20)\n"
            + "ORDER BY `SAL` DESC\n"
            + "LIMIT 5";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests prefix wildcard pattern matching. */
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

  /** Tests suffix wildcard pattern matching. */
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

  /** Tests contains wildcard pattern matching. */
  @Test
  public void testFieldsWithContainsWildcard() {
    String ppl = "source=EMP | fields *A*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], HIREDATE=[$4], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `HIREDATE`, `SAL`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests mixed explicit fields and wildcard patterns. */
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

  /** Tests mixed comma and space delimiters. */
  @Test
  public void testFieldsWithMixedDelimiters() {
    String ppl = "source=EMP | fields EMPNO ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /** Tests mixed delimiters with wildcards. */
  @Test
  public void testFieldsWithMixedDelimitersAndWildcards() {
    String ppl = "source=EMP | fields JOB EMP*, *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `JOB`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
