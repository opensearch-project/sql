/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote.nonfallback;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.calcite.remote.fallback.CalciteQueryAnalysisIT;

public class NonFallbackCalciteQueryAnalysisIT extends CalciteQueryAnalysisIT {
  @Override
  public void init() throws Exception {
    super.init();
    disallowCalciteFallback();
  }

  @Override
  @Test
  public void nonexistentFieldShouldFailSemanticCheck() {
    String query = String.format("search source=%s | fields name", TEST_INDEX_ACCOUNT);
    try {
      executeQuery(query);
      fail("Expected to throw Exception, but none was thrown for query: " + query);
    } catch (ResponseException e) {
      String errorMsg = e.getMessage();
      assertTrue(errorMsg.contains("IllegalArgumentException"));
      assertTrue(errorMsg.contains("field [name] not found"));
    } catch (IOException e) {
      throw new IllegalStateException("Unexpected exception raised for query: " + query);
    }
  }
}
