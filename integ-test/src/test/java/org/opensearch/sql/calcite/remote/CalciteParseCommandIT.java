/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.sql.ppl.ParseCommandIT;

public class CalciteParseCommandIT extends ParseCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void testParseErrorUnderscoreInGroupNames() throws IOException {
    try {
      executeQuery(
          String.format(
              "source=%s | parse email '.+@(?<host_name>.+)' | fields email", TEST_INDEX_BANK));
      fail("Should have thrown an exception for underscore in named capture group");
    } catch (Exception e) {
      assertTrue(
          e.getMessage()
              .contains("Underscores are not permitted in Java Regex capture group names"));
    }
  }
}
