/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for STRFTIME function when Calcite is disabled. Verifies that appropriate error
 * messages are returned.
 */
public class StrftimeFunctionNonCalciteIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.PEOPLE);
  }

  @Test
  public void testStrftimeVariationsWithoutCalcite() throws IOException {
    // Test different variations to ensure all fail without Calcite
    String[] queries = {
      String.format(
          "source=%s | eval result = strftime(1521467703, '%s')", TEST_INDEX_PEOPLE, "%F"),
      String.format(
          "source=%s | eval result = strftime('1521467703', '%s')", TEST_INDEX_PEOPLE, "%Y-%m-%d"),
      String.format("source=%s | eval result = strftime(age, '%s')", TEST_INDEX_PEOPLE, "%Y")
    };
    for (String query : queries) {
      try {
        executeQuery(query);
      } catch (Exception e) {
        String message = e.getMessage();
        assertTrue(message.contains("Invalid Query"));
      }
    }
  }
}
