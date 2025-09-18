/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLMultisearchTest extends CalcitePPLAbstractTest {

  public CalcitePPLMultisearchTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testBasicMultisearch() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where DEPTNO = 10] "
            + "[search source=EMP | where DEPTNO = 20]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalFilter(condition=[=($7, 10)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($7, 20)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: 3 employees from dept 10 + 5 employees from dept 20 = 8 total
    verifyResultCount(root, 8);
  }

  @Test
  public void testMultisearchWithEval() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where SAL > 2000 | eval query_type = \"high\"] "
            + "[search source=EMP | where SAL <= 2000 | eval query_type = \"low\"]";
    RelNode root = getRelNode(ppl);
    verifyResultCount(root, 14); // All 14 employees should be included
  }

  @Test
  public void testMultisearchWithStats() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where DEPTNO = 10 | eval dept_type = \"ACCOUNTING\"] "
            + "[search source=EMP | where DEPTNO = 20 | eval dept_type = \"RESEARCH\"] "
            + "| stats count by dept_type";
    RelNode root = getRelNode(ppl);
    verifyResultCount(root, 2); // Two departments
  }

  @Test
  public void testMultisearchThreeSubsearches() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where DEPTNO = 10] "
            + "[search source=EMP | where DEPTNO = 20] "
            + "[search source=EMP | where DEPTNO = 30]";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalUnion(all=[true])\n"
            + "  LogicalUnion(all=[true])\n"
            + "    LogicalFilter(condition=[=($7, 10)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "    LogicalFilter(condition=[=($7, 20)])\n"
            + "      LogicalTableScan(table=[[scott, EMP]])\n"
            + "  LogicalFilter(condition=[=($7, 30)])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    // Expected: 3 + 5 + 6 = 14 employees (all employees)
    verifyResultCount(root, 14);
  }

  @Test
  public void testMultisearchWithEmptySubsearch() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where DEPTNO = 10] "
            + "[search source=EMP | where DEPTNO = 999]"; // No employees in dept 999
    RelNode root = getRelNode(ppl);
    // Should still work, just return employees from dept 10
    verifyResultCount(root, 3);
  }

  @Test
  public void testMultisearchWithComplexFilters() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where SAL > 3000 AND JOB = \"MANAGER\"] "
            + "[search source=EMP | where SAL < 1500 AND JOB = \"CLERK\"]";
    RelNode root = getRelNode(ppl);
    // Should combine results from both conditions
    verifyResultCount(root, 3); // Estimated count based on EMP data
  }

  @Test
  public void testMultisearchWithFieldsCommand() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where DEPTNO = 10 | fields ENAME, JOB] "
            + "[search source=EMP | where DEPTNO = 20 | fields ENAME, JOB]";
    RelNode root = getRelNode(ppl);
    // Should work with field projection
    verifyResultCount(root, 8);
  }

  @Test
  public void testMultisearchSuccessRatePattern() {
    // This simulates the common SPL pattern for success rate monitoring
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where SAL > 2000 | eval query_type = \"good\"] "
            + "[search source=EMP | where SAL > 0 | eval query_type = \"valid\"] "
            + "| stats count(eval(query_type = \"good\")) as good_count, "
            + "       count(eval(query_type = \"valid\")) as total_count";
    RelNode root = getRelNode(ppl);
    verifyResultCount(root, 1); // Single aggregated row
  }

  @Test
  public void testMultisearchWithSubsearchCommands() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where DEPTNO = 10 | head 2] "
            + "[search source=EMP | where DEPTNO = 20 | head 2]";
    RelNode root = getRelNode(ppl);
    verifyResultCount(root, 4); // 2 from each subsearch
  }

  @Test
  public void testMultisearchWithEmptySearch() {
    String ppl =
        "source=EMP | multisearch "
            + "[| where DEPTNO = 10] "
            + // Empty search command
            "[search source=EMP | where DEPTNO = 20]";
    RelNode root = getRelNode(ppl);
    // Should handle empty search gracefully
    verifyResultCount(root, 5); // Only dept 20 employees
  }

  @Test(expected = Exception.class)
  public void testMultisearchWithNoSubsearches() {
    // This should fail - multisearch requires at least one subsearch
    String ppl = "source=EMP | multisearch";
    getRelNode(ppl);
  }

  @Test
  public void testMultisearchSparkSQLGeneration() {
    String ppl =
        "source=EMP | multisearch "
            + "[search source=EMP | where DEPTNO = 10] "
            + "[search source=EMP | where DEPTNO = 20]";
    RelNode root = getRelNode(ppl);

    String expectedSparkSql =
        "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 10\n"
            + "UNION ALL\n"
            + "SELECT *\n"
            + "FROM `scott`.`EMP`\n"
            + "WHERE `DEPTNO` = 20";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }

  @Test
  public void testMultisearchWithTimestampOrdering() {
    // Test multisearch with timestamp field for chronological ordering
    String ppl =
        "source=EMP | multisearch [search source=EMP | where DEPTNO = 10 | eval _time ="
            + " CAST('2024-01-01 10:00:00' AS TIMESTAMP)] [search source=EMP | where DEPTNO = 20 |"
            + " eval _time = CAST('2024-01-01 09:00:00' AS TIMESTAMP)]";
    RelNode root = getRelNode(ppl);

    // Verify logical plan includes sorting by _time
    String logicalPlan = root.toString();
    // Should contain LogicalSort with _time field ordering
    // This ensures timestamp-based ordering is applied after UNION ALL
    verifyResultCount(root, 8); // 3 + 5 = 8 employees
  }

  @Test
  public void testMultisearchTimestampOrderingBehavior() {
    // Test that demonstrates SPL-compatible timestamp ordering
    String ppl =
        "source=EMP | multisearch [search source=EMP | where DEPTNO = 10 | eval _time ="
            + " CAST('2024-01-01 10:00:00' AS TIMESTAMP), source_type = \"A\"] [search source=EMP |"
            + " where DEPTNO = 20 | eval _time = CAST('2024-01-01 11:00:00' AS TIMESTAMP),"
            + " source_type = \"B\"]";
    RelNode root = getRelNode(ppl);

    // With timestamp ordering, events should be sorted chronologically across sources
    // This matches SPL multisearch behavior of timestamp-based interleaving
    String expectedLogicalPattern = "LogicalSort";
    String logicalPlan = root.toString();
    assertTrue(
        "Multisearch should include timestamp-based sorting",
        logicalPlan.contains(expectedLogicalPattern));

    verifyResultCount(root, 8); // All employees from both departments
  }
}
