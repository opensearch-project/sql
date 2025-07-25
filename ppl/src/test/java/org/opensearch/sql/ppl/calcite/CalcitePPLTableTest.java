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

    // Test comma-delimited syntax
    ppl = "source=EMP | table EMPNO,ENAME";
    root = getRelNode(ppl);
    verifyLogical(root, expectedLogical);
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
   * Tests selecting all fields using the wildcard (*) syntax. Verifies that 'table *' is correctly
   * translated to SELECT * in SQL.
   */
  @Test
  public void testTableWildcardAllFields() {
    String ppl = "source=EMP | table *";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(COMM=[$6], DEPTNO=[$7], EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], JOB=[$2],"
            + " MGR=[$3], SAL=[$5])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `COMM`, `DEPTNO`, `EMPNO`, `ENAME`, `HIREDATE`, `JOB`, `MGR`, `SAL`\n"
            + "FROM `scott`.`EMP`";
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
   * Tests basic field selection with multiple columns. Extends testTableBasicFields with an
   * additional column to verify handling of more fields.
   */
  @Test
  public void testTableMultipleFields() {
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
   * fields are correctly handled with wildcards and duplicates are removed.
   */
  @Test
  public void testTableWithRenameAndWildcard() {
    String ppl =
        "source=EMP | rename EMPNO as emp_id, ENAME as emp_name | table emp_id, emp_name, emp*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(emp_id=[$0], emp_name=[$1])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO` `emp_id`, `ENAME` `emp_name`\n" + "FROM `scott`.`EMP`";
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
   * that fields containing 'E' are correctly selected and duplicates are removed.
   */
  @Test
  public void testTableWithWildcardPattern() {
    String ppl = "source=EMP | table ENAME, *E*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], EMPNO=[$0], HIREDATE=[$4])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, `EMPNO`, `HIREDATE`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests wildcard pattern matching for fields with a specific prefix. This extends
   * testTableWildcardFieldsStarting by combining explicit fields with wildcard patterns.
   */
  @Test
  public void testTableWithSpecificPrefixWildcard() {
    String ppl = "source=EMP | table ENAME, JOB, DEPT*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `JOB`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests mixed field specification with regular fields and wildcards. This test specifically
   * focuses on interspersing wildcards between regular fields in a specific order.
   */
  @Test
  public void testTableWithMixedFieldSpecification() {
    String ppl = "source=EMP | table ENAME, SAL*, JOB, HIREDATE";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], SAL=[$5], JOB=[$2], HIREDATE=[$4])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `SAL`, `JOB`, `HIREDATE`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests table command with multiple wildcards matching different patterns. This test verifies
   * that fields ending with 'NAME' and 'NO' are correctly selected along with JOB.
   */
  @Test
  public void testTableWithMultipleWildcardTypes() {
    String ppl = "source=EMP | table *NAME, *NO, JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], EMPNO=[$0], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `DEPTNO`, `EMPNO`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests multiple wildcard patterns in field selection. This test specifically focuses on using
   * multiple different wildcard patterns in a single query with deduplication.
   */
  @Test
  public void testTableWithMultipleWildcardPatterns() {
    String ppl = "source=EMP | table ENAME, E*, D*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests space-delimited only syntax without commas. Verifies that fields can be specified with
   * spaces only, without requiring commas as separators.
   */
  @Test
  public void testTableSpaceDelimitedOnly() {
    String ppl = "source=EMP | table ENAME JOB DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], JOB=[$2], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `JOB`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests space-delimited syntax with wildcard field selection. Verifies that fields starting with
   * 'EMP' are correctly selected using space-delimited syntax.
   */
  @Test
  public void testTableSpaceDelimitedWildcardFieldsStarting() {
    String ppl = "source=EMP | table EMP*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests space-delimited syntax with multiple wildcard patterns. Verifies that multiple wildcard
   * patterns work correctly with space-delimited syntax and duplicates are removed.
   */
  @Test
  public void testTableSpaceDelimitedMultipleWildcards() {
    String ppl = "source=EMP | table ENAME E* D*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], EMPNO=[$0], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests space-delimited syntax with a filter operation. Verifies that filtering works correctly
   * before field selection with space-delimited syntax.
   */
  @Test
  public void testTableSpaceDelimitedWithFilter() {
    String ppl = "source=EMP | where DEPTNO = 20 | table EMPNO ENAME SAL";
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
   * Tests space-delimited syntax with a sort operation. Verifies that sorting is applied before
   * field selection with space-delimited syntax.
   */
  @Test
  public void testTableSpaceDelimitedWithSort() {
    String ppl = "source=EMP | sort EMPNO | table EMPNO ENAME";
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
   * Tests space-delimited syntax with a complex query. Verifies that multiple operations work
   * correctly with space-delimited field selection.
   */
  @Test
  public void testTableSpaceDelimitedWithComplexQuery() {
    String ppl = "source=EMP | where SAL > 1000 | sort - SAL | table ENAME SAL DEPTNO | head 3";
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
   * Tests that field ordering in the space-delimited table command is preserved in the output.
   * Verifies that fields appear in the result in the same order as specified.
   */
  @Test
  public void testTableSpaceDelimitedFieldOrdering() {
    String ppl = "source=EMP | table JOB ENAME EMPNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(JOB=[$2], ENAME=[$1], EMPNO=[$0])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `JOB`, `ENAME`, `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests comma-delimited syntax with wildcard patterns. Verifies that comma-delimited syntax works
   * correctly with wildcard patterns.
   */
  @Test
  public void testTableCommaDelimitedWithWildcards() {
    String ppl = "source=EMP | table ENAME,*NO,JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], EMPNO=[$0], JOB=[$2])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `ENAME`, `DEPTNO`, `EMPNO`, `JOB`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests the best practice of placing the table command at the end of search pipelines. This
   * demonstrates the recommended pattern for optimal performance.
   */
  @Test
  public void testTableAsLastCommand() {
    String ppl =
        "source=EMP | where SAL > 1000 | sort DEPTNO | eval ratio = SAL/1000 | table ENAME, DEPTNO,"
            + " ratio";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$1], DEPTNO=[$7], ratio=[DIVIDE($5, 1000)])\n"
            + "  LogicalSort(sort0=[$7], dir0=[ASC-nulls-first])\n"
            + "    LogicalFilter(condition=[>($5, 1000)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `DEPTNO`, `DIVIDE`(`SAL`, 1000) `ratio`\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `SAL` > 1000\n"
            + "ORDER BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests using the fields command for filtering operations and table for presentation. This
   * demonstrates the recommended practice of using fields for filtering and table for final
   * display.
   */
  @Test
  public void testFieldsForFilteringTableForPresentation() {
    String ppl =
        "source=EMP | fields ENAME, JOB, SAL, DEPTNO | where SAL > 2000 | table ENAME, JOB, DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$0], JOB=[$1], DEPTNO=[$3])\n"
            + "  LogicalFilter(condition=[>($2, 2000)])\n"
            + "    LogicalProject(ENAME=[$1], JOB=[$2], SAL=[$5], DEPTNO=[$7])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `JOB`, `DEPTNO`\n"
            + "FROM (SELECT `ENAME`, `JOB`, `SAL`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "WHERE `SAL` > 2000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests selecting a large number of fields with wildcards for performance optimization. This
   * demonstrates how to efficiently select related fields using wildcards.
   */
  @Test
  public void testTableWithLargeFieldSetOptimization() {
    String ppl = "source=EMP | table E*, *DATE, D*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1], HIREDATE=[$4], DEPTNO=[$7])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `EMPNO`, `ENAME`, `HIREDATE`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests compatibility with other PPL commands in a complex pipeline. This demonstrates how table
   * integrates with the existing PPL command architecture.
   */
  @Test
  public void testTableCompatibilityWithOtherCommands() {
    String ppl =
        "source=EMP | stats count() as cnt, avg(SAL) as avgSal by DEPTNO | sort - avgSal | table"
            + " DEPTNO, avgSal, cnt";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(DEPTNO=[$2], avgSal=[$1], cnt=[$0])\n"
            + "  LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "    LogicalProject(cnt=[$1], avgSal=[$2], DEPTNO=[$0])\n"
            + "      LogicalAggregate(group=[{0}], cnt=[COUNT()], avgSal=[AVG($1)])\n"
            + "        LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `DEPTNO`, `avgSal`, `cnt`\n"
            + "FROM (SELECT COUNT(*) `cnt`, AVG(`SAL`) `avgSal`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `DEPTNO`\n"
            + "ORDER BY 2 DESC) `t2`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests table command in the middle of a pipeline followed by other commands. This verifies that
   * table can be used to select fields before further processing.
   */
  @Test
  public void testTableInMiddleOfPipeline() {
    String ppl = "source=EMP | table ENAME, SAL, DEPTNO | where SAL > 2000 | sort - SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])\n"
            + "  LogicalFilter(condition=[>($1, 2000)])\n"
            + "    LogicalProject(ENAME=[$1], SAL=[$5], DEPTNO=[$7])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `ENAME`, `SAL`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "WHERE `SAL` > 2000\n"
            + "ORDER BY `SAL` DESC";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests multiple table commands in a pipeline. This verifies that table commands can be used to
   * progressively refine field selection.
   */
  @Test
  public void testMultipleTableCommands() {
    String ppl = "source=EMP | table ENAME, SAL, DEPTNO, JOB | where SAL > 2000 | table ENAME, SAL";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(ENAME=[$0], SAL=[$1])\n"
            + "  LogicalFilter(condition=[>($1, 2000)])\n"
            + "    LogicalProject(ENAME=[$1], SAL=[$5], DEPTNO=[$7], JOB=[$2])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT `ENAME`, `SAL`\n"
            + "FROM (SELECT `ENAME`, `SAL`, `DEPTNO`, `JOB`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "WHERE `SAL` > 2000";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests table command with wildcard patterns in the middle of a pipeline. This verifies that
   * wildcard field selection works correctly when not at the end of the pipeline.
   */
  @Test
  public void testTableWithWildcardInMiddleOfPipeline() {
    String ppl = "source=EMP | table EMP*, SAL, DEPT* | where SAL > 2000 | sort DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalSort(sort0=[$2], dir0=[ASC-nulls-first])\n"
            + "  LogicalFilter(condition=[>($1, 2000)])\n"
            + "    LogicalProject(EMPNO=[$0], SAL=[$5], DEPTNO=[$7])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM (SELECT `EMPNO`, `SAL`, `DEPTNO`\n"
            + "FROM `scott`.`EMP`) `t`\n"
            + "WHERE `SAL` > 2000\n"
            + "ORDER BY `DEPTNO`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests field deduplication when multiple wildcard patterns match the same field. Verifies that
   * EMPNO appears only once even though both EMP* and *NO patterns match it.
   */
  @Test
  public void testTableFieldDeduplicationWithWildcards() {
    String ppl = "source=EMP | table EMP*, *NO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], DEPTNO=[$7])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `DEPTNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests field deduplication with explicit field and wildcard pattern. Verifies that EMPNO appears
   * only once when explicitly listed and also matched by wildcard.
   */
  @Test
  public void testTableFieldDeduplicationExplicitAndWildcard() {
    String ppl = "source=EMP | table EMPNO, EMP*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  /**
   * Tests field deduplication with multiple overlapping patterns. Verifies that ENAME appears only
   * once despite being matched by multiple patterns.
   */
  @Test
  public void testTableFieldDeduplicationMultipleOverlaps() {
    String ppl = "source=EMP | table E*, *NAME, EN*";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(EMPNO=[$0], ENAME=[$1])\n" + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);

    String expectedSparkSql = "SELECT `EMPNO`, `ENAME`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
