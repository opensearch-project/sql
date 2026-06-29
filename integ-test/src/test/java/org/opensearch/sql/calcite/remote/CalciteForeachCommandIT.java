/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteForeachCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    if (!TestUtils.isIndexExist(client(), "test_foreach")) {
      String mapping =
          "{\"mappings\":{\"properties\":{"
              + "\"a\":{\"type\":\"long\"},"
              + "\"b\":{\"type\":\"long\"},"
              + "\"value_cpu\":{\"type\":\"long\"},"
              + "\"value_mem\":{\"type\":\"long\"}}}}";
      TestUtils.createIndexByRestClient(client(), "test_foreach", mapping);

      Request request1 = new Request("PUT", "/test_foreach/_doc/1?refresh=true");
      request1.setJsonEntity("{\"a\": 1, \"b\": 2, \"value_cpu\": 10, \"value_mem\": 20}");
      client().performRequest(request1);

      Request request2 = new Request("PUT", "/test_foreach/_doc/2?refresh=true");
      request2.setJsonEntity("{\"a\": 3, \"b\": 4, \"value_cpu\": 30, \"value_mem\": 40}");
      client().performRequest(request2);
    }
  }

  @Test
  public void testForeachExplicitFields() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | foreach a b [ eval <<FIELD>>_double = <<FIELD>> * 2 ] |"
                + " fields a_double, b_double");
    verifySchema(result, schema("a_double", "bigint"), schema("b_double", "bigint"));
    verifyDataRows(result, rows(2, 4), rows(6, 8));
  }

  @Test
  public void testForeachWildcardMatchstr() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach | foreach value_* [ eval copy_<<MATCHSTR>> = <<FIELD>> ] |"
                + " fields copy_cpu, copy_mem");
    verifySchema(result, schema("copy_cpu", "bigint"), schema("copy_mem", "bigint"));
    verifyDataRows(result, rows(10, 20), rows(30, 40));
  }
}
