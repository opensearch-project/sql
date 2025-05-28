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
import org.junit.After;
import org.junit.jupiter.api.Test;

public class RareCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    setQuerySizeLimit(2000);
  }

  @After
  public void afterTest() throws IOException {
    resetQuerySizeLimit();
  }

  @Test
  public void testRareWithoutGroup() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s | rare gender", TEST_INDEX_ACCOUNT));
    if (isCalciteEnabled()) {
      verifySchemaInOrder(result, schema("gender", "string"), schema("count", "bigint"));
      verifyDataRows(result, rows("F", 493), rows("M", 507));
    } else {
      verifyDataRows(result, rows("F"), rows("M"));
    }
  }

  @Test
  public void testRareWithGroup() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | rare state by gender", TEST_INDEX_ACCOUNT));
    if (isCalciteEnabled()) {
      verifySchemaInOrder(
          result, schema("gender", "string"), schema("state", "string"), schema("count", "bigint"));
      verifyDataRows(
          result,
          rows("F", "DE", 3),
          rows("F", "WI", 5),
          rows("F", "OR", 5),
          rows("F", "CT", 5),
          rows("F", "WA", 6),
          rows("F", "SC", 6),
          rows("F", "OK", 7),
          rows("F", "KS", 7),
          rows("F", "CO", 7),
          rows("F", "NV", 8),
          rows("M", "NE", 5),
          rows("M", "RI", 5),
          rows("M", "NV", 5),
          rows("M", "MI", 5),
          rows("M", "MT", 6),
          rows("M", "AZ", 6),
          rows("M", "NM", 6),
          rows("M", "SD", 6),
          rows("M", "KY", 6),
          rows("M", "IN", 6));
    } else {
      verifyDataRows(
          result,
          rows("F", "DE"),
          rows("F", "WI"),
          rows("F", "OR"),
          rows("F", "CT"),
          rows("F", "WA"),
          rows("F", "SC"),
          rows("F", "OK"),
          rows("F", "KS"),
          rows("F", "CO"),
          rows("F", "VA"),
          rows("M", "NE"),
          rows("M", "RI"),
          rows("M", "NV"),
          rows("M", "MI"),
          rows("M", "MT"),
          rows("M", "AZ"),
          rows("M", "NM"),
          rows("M", "SD"),
          rows("M", "KY"),
          rows("M", "IN"));
    }
  }
}
