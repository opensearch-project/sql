/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;

import java.io.IOException;
import org.junit.jupiter.api.Test;

public class RelevanceFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  @Test
  public void test1() throws IOException {
    String query = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string([\\\"Tags\\\" ^ 1.5, Title, `Body` 4.2], 'taste')";
    var result = executeQuery(query);
    //verifyDataRows(result, rows(32));

    assertNotEquals(0, result.getInt("total"));
  }

  @Test
  public void verify_wildcard_test() throws IOException {
    String query1 = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string(['Tags'], 'taste')";
    var result1 = executeQuery(query1);
    String query2 = "SOURCE=" + TEST_INDEX_BEER
        + " | WHERE simple_query_string(['T*'], 'taste')";
    var result2 = executeQuery(query2);
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));
  }
}
