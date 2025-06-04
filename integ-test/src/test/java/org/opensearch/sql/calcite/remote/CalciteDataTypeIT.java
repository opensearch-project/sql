/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.DataTypeIT;

import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NONNUMERIC;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

public class CalciteDataTypeIT extends DataTypeIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Test
  @Override
  public void test_nonnumeric_data_types() throws IOException {
    JSONObject result = executeQuery(String.format("source=%s", TEST_INDEX_DATATYPE_NONNUMERIC));
    verifySchemaInOrder(
            result,
            schema("text_value", "string"),
            schema("date_nanos_value", "timestamp"),
            schema("date_value", "timestamp"),
            schema("boolean_value", "boolean"),
            schema("ip_value", "ip"),
            schema("nested_value", "array"),
            schema("object_value", "struct"),
            schema("keyword_value", "string"),
            schema("geo_point_value", "geo_point"),
            schema("binary_value", "binary"));
    verifyDataRowsInOrder(
            result,
            rows(
                    "text",
                    "2019-03-24 01:34:46.123456789",
                    "2020-10-13 13:00:00",
                    true,
                    "127.0.0.1",
                    new JSONArray(
                            "[{\"last\": \"Smith\", \"first\": \"John\"}, {\"last\": \"White\", \"first\":"
                                    + " \"Alice\"}]"),
                    new JSONObject("{\"last\": \"Dale\", \"first\": \"Dale\"}"),
                    "keyword",
                    new JSONObject("{\"lon\": 74, \"lat\": 40.71}"),
                    "U29tZSBiaW5hcnkgYmxvYg=="));
  }
}
