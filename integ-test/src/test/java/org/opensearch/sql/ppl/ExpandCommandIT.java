/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

public class ExpandCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.NESTED_SIMPLE);
  }

  @Test
  public void testExpand() throws Exception {
    JSONObject response =
        executeQuery(String.format("source=%s | expand address", TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        response,
        schema("name", "string"),
        schema("age", "integer"),
        schema("id", "integer"),
        schema("address", "object"));
    verifyNumOfRows(response, 11);
  }

  // TODO: double check if expand with alias is supported
  @Ignore
  @Test
  public void testExpandWithAlias() throws Exception {
    JSONObject response =
        executeQuery(String.format("source=%s | expand address as addr", TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        response,
        schema("name", "string"),
        schema("age", "integer"),
        schema("id", "integer"),
        schema("address", "array"),
        schema("addr", "object"));
    verifyNumOfRows(response, 11);
  }
}
