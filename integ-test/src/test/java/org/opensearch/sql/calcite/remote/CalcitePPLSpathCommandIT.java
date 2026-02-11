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
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLSpathCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);

    // Create test data for string concatenation
    Request request1 = new Request("PUT", "/test_spath/_doc/1?refresh=true");
    request1.setJsonEntity("{\"doc\": \"{\\\"n\\\": 1}\"}");
    client().performRequest(request1);

    Request request2 = new Request("PUT", "/test_spath/_doc/2?refresh=true");
    request2.setJsonEntity("{\"doc\": \"{\\\"n\\\": 2}\"}");
    client().performRequest(request2);

    Request request3 = new Request("PUT", "/test_spath/_doc/3?refresh=true");
    request3.setJsonEntity("{\"doc\": \"{\\\"n\\\": 3}\"}");
    client().performRequest(request3);
  }

  @Test
  public void testSimpleSpath() throws IOException {
    JSONObject result =
        executeQuery("source=test_spath | spath input=doc output=result path=n | fields result");
    verifySchema(result, schema("result", "string"));
    verifyDataRows(result, rows("1"), rows("2"), rows("3"));
  }

  @Test
  public void testSpathWithMvCombine() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_json | where category='simple' "
                + "| spath input=userData "
                + "| fields a, b, c "
                + "| mvcombine c");

    verifySchema(result, schema("a", "string"), schema("b", "string"), schema("c", "array"));

    verifyDataRows(result, rows("1", "2", new String[] {"3", "3"}));
  }
}
