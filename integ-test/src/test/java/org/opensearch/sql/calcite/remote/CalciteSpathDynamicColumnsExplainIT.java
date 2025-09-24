/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.sql.ppl.ExplainIT;

public class CalciteSpathDynamicColumnsExplainIT extends ExplainIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK_WITH_STRING_VALUES);
  }

  @Test
  public void testSpathDynamicColumnsPhysicalPlan() throws IOException {
    // Test spath command with dynamic columns to trace physical plan execution
    // Use correct syntax with input parameter and a field that contains JSON data
    String query =
        "source=opensearch-sql_test_index_bank | spath input=address | fields name, age, city";
    var result = explainQueryToString(query);

    // Print the physical plan to console for debugging
    System.out.println("=== SPATH DYNAMIC COLUMNS PHYSICAL PLAN ===");
    System.out.println(result);
    System.out.println("=== END PHYSICAL PLAN ===");

    // For now, just verify the plan contains expected elements
    // We'll analyze the output to understand where column conversion happens
    assertTrue("Physical plan should contain LogicalProject", result.contains("LogicalProject"));
  }

  @Test
  public void testSpathDynamicColumnsWithExplicitFields() throws IOException {
    // Test with explicit field selection to see how column names are handled
    String query =
        "source=opensearch-sql_test_index_bank | spath input=address | fields address.name,"
            + " address.age";
    var result = explainQueryToString(query);

    System.out.println("=== SPATH WITH EXPLICIT FIELDS PHYSICAL PLAN ===");
    System.out.println(result);
    System.out.println("=== END PHYSICAL PLAN ===");

    assertTrue("Physical plan should contain LogicalProject", result.contains("LogicalProject"));
  }

  @Test
  public void testSpathDynamicColumnsSimple() throws IOException {
    // Simplest possible spath test to isolate the issue
    String query = "source=opensearch-sql_test_index_bank | spath input=address";
    var result = explainQueryToString(query);

    System.out.println("=== SIMPLE SPATH PHYSICAL PLAN ===");
    System.out.println(result);
    System.out.println("=== END PHYSICAL PLAN ===");

    assertTrue("Physical plan should contain some logical operation", result.contains("Logical"));
  }
}
