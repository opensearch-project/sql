/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class TopCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    setQuerySizeLimit(2000);
  }

  @Test
  public void testTopWithoutGroup() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | top gender", TEST_INDEX_ACCOUNT));
    if (isCalciteEnabled()) {
      verifySchemaInOrder(result, schema("gender", "string"), schema("count", "bigint"));
      verifyDataRows(result, rows("M", 507), rows("F", 493));
    } else {
      verifyDataRows(result, rows("M"), rows("F"));
    }
  }

  @Test
  public void testTopNWithoutGroup() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | top 1 gender", TEST_INDEX_ACCOUNT));
    if (isCalciteEnabled()) {
      verifySchemaInOrder(result, schema("gender", "string"), schema("count", "bigint"));
      verifyDataRows(result, rows("M", 507));
    } else {
      verifyDataRows(result, rows("M"));
    }
  }

  @Test
  public void testTopNWithGroup() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | top 1 state by gender", TEST_INDEX_ACCOUNT));
    if (isCalciteEnabled()) {
      verifySchemaInOrder(
          result, schema("gender", "string"), schema("state", "string"), schema("count", "bigint"));
      verifyDataRows(result, rows("F", "TX", 17), rows("M", "MD", 18));
    } else {
      verifyDataRows(result, rows("F", "TX"), rows("M", "MD"));
    }
  }
}
