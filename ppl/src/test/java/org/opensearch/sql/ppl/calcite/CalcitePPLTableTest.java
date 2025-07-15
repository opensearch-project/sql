/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/**
 * Test class for validating the table command functionality in PPL (Piped Processing Language).
 * This class tests various scenarios of the 'table' command which is used to select specific fields
 * from the result set, similar to the SELECT clause in SQL.
 *
 * <p>The tests verify both the logical plan generation and the translation to Spark SQL syntax.
 */
public class CalcitePPLTableTest extends CalcitePPLAbstractTest {

  /**
   * Constructor that initializes the test with the SCOTT schema which includes temporal data. This
   * schema provides standard test tables like EMP and DEPT with predefined data.
   */
  public CalcitePPLTableTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  /**
   * Tests basic field selection using the table command. Verifies that a simple table command
   * correctly selects specified columns.
   */
  @Test
  public void testTableBasicFields() {
    String ppl = "source=EMP | table EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests the table command when used after a filter operation. Verifies that filtering works
   * correctly before field selection.
   */
  @Test
  public void testTableWithFilter() {
    String ppl = "source=EMP | where DEPTNO = 20 | table EMPNO, ENAME, SAL";
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

  /**
   * Tests selecting a single field using the table command. Verifies that the command works
   * correctly with just one column.
   */
  @Test
  public void testTableSingleField() {
    String ppl = "source=EMP | table ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests the table command when used after a sort operation. Verifies that sorting is applied
   * before field selection.
   */
  @Test
  public void testTableWithSort() {
    String ppl = "source=EMP | sort EMPNO | table EMPNO, ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "  LogicalSort(sort0=[$0], dir0=[ASC-nulls-first])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`\n" + "ORDER BY `EMPNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests the table command followed by a limit operation (head). Verifies that the limit is
   * correctly applied after field selection.
   */
  @Test
  public void testTableWithLimit() {
    String ppl = "source=EMP | table EMPNO, ENAME | head 5";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(fetch=[5])\n"
            + "  LogicalProject(EMPNO=[$0], ENAME=[$1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`\n" + "LIMIT 5";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests selecting all fields explicitly using the table command. Verifies that when all fields
   * are listed, it's equivalent to SELECT *.
   */
  @Test
  public void testTableAllFields() {
    String ppl = "source=EMP | table EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5],"
            + " COMM=[$6], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT *\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests a complex query with multiple operations including filter, sort, table, and limit.
   * Verifies that all operations are correctly combined in the expected order.
   */
  @Test
  public void testTableWithComplexQuery() {
    String ppl = "source=EMP | where SAL > 1000 | sort - SAL | table ENAME, SAL, DEPTNO | head 3";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], SAL=[$5], DEPTNO=[$7])\n"
            + "  LogicalSort(sort0=[$5], dir0=[DESC-nulls-last], fetch=[3])\n"
            + "    LogicalFilter(condition=[>($5, 1000)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `SAL`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` > 1000\n"
            + "ORDER BY `SAL` DESC\n"
            + "LIMIT 3";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests basic field selection with multiple columns. Similar to testTableBasicFields but with an
   * additional column.
   */
  @Test
  public void testTableBasicFieldSelection() {
    String ppl = "source=EMP | table EMPNO, ENAME, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests wildcard field selection where the pattern matches the beginning of field names. Verifies
   * that fields starting with 'EMP' are correctly selected.
   */
  @Test
  public void testTableWildcardFieldsStarting() {
    String ppl = "source=EMP | table EMP*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests wildcard field selection where the pattern matches the end of field names. Verifies that
   * fields ending with 'NO' are correctly selected.
   */
  @Test
  public void testTableWildcardFieldsEnding() {
    String ppl = "source=EMP | table *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$7], EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `DEPTNO`, `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests a mix of explicit field names and wildcard patterns. Verifies that both explicitly named
   * fields and wildcard matches are included.
   */
  @Test
  public void testTableMixedWildcardAndExplicit() {
    String ppl = "source=EMP | table ENAME, EMP*, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], EMPNO=[$0], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `EMPNO`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests that field ordering in the table command is preserved in the output. Verifies that fields
   * appear in the result in the same order as specified.
   */
  @Test
  public void testTableFieldOrdering() {
    String ppl = "source=EMP | table JOB, ENAME, EMPNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], ENAME=[$1], EMPNO=[$0])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `JOB`, `ENAME`, `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests combining filter operations with wildcard field selection. Verifies that filtering works
   * correctly with wildcard field patterns.
   */
  @Test
  public void testTableWithFilterAndWildcard() {
    String ppl = "source=EMP | where DEPTNO = 10 | table ENAME, EMP*, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], EMPNO=[$0], JOB=[$2])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `EMPNO`, `JOB`\n" + "FROM `scott`.`EMP`\n" + "WHERE `DEPTNO` = 10";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests the interaction between field renaming and wildcard patterns. Verifies that renamed
   * fields are correctly handled with wildcards.
   */
  @Test
  public void testTableWithRenameAndWildcard() {
    String ppl =
        "source=EMP | rename EMPNO as emp_id, ENAME as emp_name | table emp_id, emp_name, emp*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(emp_id=[$0], emp_name=[$1], emp_id0=[$0], emp_name0=[$1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO` `emp_id`, `ENAME` `emp_name`, `EMPNO` `emp_id0`, `ENAME` `emp_name0`\n"
            + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests table command with deduplication and field evaluation. Verifies that dedup and eval
   * operations work correctly with field selection.
   */
  @Test
  public void testTableWithDedupAndEval() {
    String ppl =
        "source=EMP | dedup DEPTNO | eval dept_type=case(DEPTNO=10, 'accounting' else 'other') |"
            + " table EMPNO, dept_type";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], dept_type=[CASE(=($7, 10), 'accounting':VARCHAR,"
            + " 'other':VARCHAR)])\n"
            + "  LogicalFilter(condition=[<=($8, 1)])\n"
            + "    LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7], _row_number_=[ROW_NUMBER() OVER (PARTITION BY $7"
            + " ORDER BY $7)])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, CASE WHEN `DEPTNO` = 10 THEN 'accounting' ELSE 'other' END `dept_type`\n"
            + "FROM (SELECT `EMPNO`, `ENAME`, `JOB`, `MGR`, `HIREDATE`, `SAL`, `COMM`, `DEPTNO`,"
            + " ROW_NUMBER() OVER (PARTITION BY `DEPTNO` ORDER BY `DEPTNO` NULLS LAST)"
            + " `_row_number_`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` IS NOT NULL) `t0`\n"
            + "WHERE `_row_number_` <= 1";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests complex wildcard patterns that match characters in the middle of field names. Verifies
   * that fields containing 'E' are correctly selected.
   */
  @Test
  public void testTableWithWildcardPattern() {
    String ppl = "source=EMP | table ENAME, *E*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], EMPNO=[$0], ENAME0=[$1], HIREDATE=[$4])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, `EMPNO`, `ENAME` `ENAME0`, `HIREDATE`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
