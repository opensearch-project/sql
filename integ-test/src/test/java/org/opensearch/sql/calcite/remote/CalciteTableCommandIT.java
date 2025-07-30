/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Calcite integration tests for table command - verifies table command works with Calcite engine.
 */
public class CalciteTableCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    enableCalcite();  // Enable Calcite execution engine
    disallowCalciteFallback();  // Ensure tests run on Calcite, not fallback
  }

  /**
   * Tests table command equivalence with fields command using Calcite engine.
   */
  @Test
  public void testCalciteTableEquivalentToFields() throws IOException {
    JSONObject fieldsResult =
        executeQuery(String.format("source=%s | fields firstname, lastname | head 3", TEST_INDEX_ACCOUNT));
    JSONObject tableResult =
        executeQuery(String.format("source=%s | table firstname, lastname | head 3", TEST_INDEX_ACCOUNT));
    
    verifySchema(fieldsResult, 
        schema("firstname", "string"), 
        schema("lastname", "string"));
    verifySchema(tableResult, 
        schema("firstname", "string"), 
        schema("lastname", "string"));
    
    verifyDataRows(fieldsResult, 
        rows("Amber", "Duke"), 
        rows("Hattie", "Bond"), 
        rows("Nanette", "Bates"));
    verifyDataRows(tableResult, 
        rows("Amber", "Duke"), 
        rows("Hattie", "Bond"), 
        rows("Nanette", "Bates"));
  }
}