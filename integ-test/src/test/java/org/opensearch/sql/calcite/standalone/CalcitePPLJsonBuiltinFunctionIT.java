/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLJsonBuiltinFunctionIT extends CalcitePPLIntegTestCase {
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
  public void testJson() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json('[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]'),"
                    + " b=json('{\"invalid\": \"json\"')| fields a,b | head 1",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(actual, schema("a", "string"), schema("b", "string"));

    verifyDataRows(actual, rows("[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]", null));
  }

  @Test
  public void testJsonObject() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = json_object('key', 123.45), b=json_object('outer',"
                    + " json_object('inner', 123.45))| fields a, b | head 1",
                TEST_INDEX_PEOPLE2));

    verifySchema(actual, schema("a", "struct"), schema("b", "struct"));

    verifyDataRows(actual, rows("[1,2,3,{\"f1\":1,\"f2\":[5,6]},4]", null));
  }
}
