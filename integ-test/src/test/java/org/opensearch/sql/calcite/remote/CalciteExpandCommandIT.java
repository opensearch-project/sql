/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ARRAY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteExpandCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.NESTED_SIMPLE);
    loadIndex(Index.ARRAY);
    enableCalcite();
    disallowCalciteFallback();
  }

  @Test
  public void testExpandOnNested() throws Exception {
    JSONObject response =
        executeQuery(String.format("source=%s | expand address", TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        response,
        schema("name", "string"),
        schema("age", "bigint"),
        schema("id", "bigint"),
        schema("address", "struct"));
    verifyNumOfRows(response, 11);
  }

  // TODO: confirm if expand on array (instead of nested) will be supported.
  //  In Opensearch, a string field can store either a single string or an array of strings.
  //  This makes it difficult to implement expand on array.
  @Ignore
  @Test
  public void testExpandOnArray() throws Exception {
    JSONObject response =
        executeQuery(String.format("source=%s | expand strings", TEST_INDEX_ARRAY));
    verifySchema(response, schema("numbers", "array"), schema("strings", "string"));
    verifyNumOfRows(response, 5);
  }

  @Test
  public void testExpandWithAlias() throws Exception {
    JSONObject response =
        executeQuery(String.format("source=%s | expand address as addr", TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        response,
        schema("name", "string"),
        schema("age", "bigint"),
        schema("id", "bigint"),
        schema("addr", "struct"));
    verifyNumOfRows(response, 11);
  }
}
