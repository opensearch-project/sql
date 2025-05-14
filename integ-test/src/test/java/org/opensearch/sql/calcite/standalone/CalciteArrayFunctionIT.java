/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.calcite.utils.BuiltinFunctionUtils.gson;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.util.Map;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalciteArrayFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.DATE_FORMATS);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.DATE);
    loadIndex(Index.PEOPLE2);
    loadIndex(Index.BANK);
  }

  @Test
  public void testForAll() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval array = array(1, -1, 2), result = forall(array, x -> x > 0) |"
                    + " fields result | head 1",
                TEST_INDEX_BANK));

    verifySchema(actual, schema("a", "struct"));

    verifyDataRows(
        actual,
        rows(
            gson.fromJson("{\"student\":[{\"name\":\"Bob\"},{\"name\":\"Charlie\"}]}", Map.class)));
  }
}
