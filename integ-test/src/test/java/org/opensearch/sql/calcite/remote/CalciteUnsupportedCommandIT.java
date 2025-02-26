/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteUnsupportedCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.BANK);
  }

  @Test
  public void test_match_function() throws IOException {
    failWithMessage(
        String.format(
            "source=%s | where match(firstname, 'Hattie') | fields firstname", TEST_INDEX_BANK),
        "Unsupported operator: match");
  }
}
