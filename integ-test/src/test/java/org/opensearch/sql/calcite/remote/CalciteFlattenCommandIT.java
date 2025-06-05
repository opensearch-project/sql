/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.NESTED_WITHOUT_ARRAYS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteFlattenCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(NESTED_WITHOUT_ARRAYS);
    enableCalcite();
    disallowCalciteFallback();
  }

  @Test
  public void testFlattenStruct() throws Exception {
    JSONObject result =
        executeQuery(
            String.format("source=%s | flatten message", TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS));
    verifySchema(
        result,
        schema("message", "struct"),
        schema("info", "string"),
        schema("author", "string"),
        schema("dayOfWeek", "integer"),
        schema("comment", "array"),
        schema("myNum", "integer"),
        schema("someField", "string"));
    verifyNumOfRows(result, 5);
  }
}
