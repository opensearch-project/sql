/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.Request;

public class CalcitePPLTopAndRareIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.OCCUPATION_TOP_RARE);

    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity(
        "{\"name\": \"Hello\", \"age\": 20, \"address\": \"Seattle\", \"year\": 2023, \"month\":"
            + " 4}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity(
        "{\"name\": \"World\", \"age\": 30, \"address\": \"Portland\", \"year\": 2023, \"month\":"
            + " 4}");
    client().performRequest(request2);
    Request request3 = new Request("PUT", "/test/_doc/3?refresh=true");
    request3.setJsonEntity(
        "{\"name\": \"Scala\", \"age\": 40, \"address\": \"Seattle\", \"year\": 2023, \"month\":"
            + " 5}");
    client().performRequest(request3);
    Request request4 = new Request("PUT", "/test/_doc/4?refresh=true");
    request4.setJsonEntity(
        "{\"name\": \"Java\", \"age\": 50, \"address\": \"Portland\", \"year\": 2023, \"month\":"
            + " 5}");
    client().performRequest(request4);
    Request request5 = new Request("PUT", "/test/_doc/5?refresh=true");
    request5.setJsonEntity(
        "{\"name\": \"Test\", \"age\": 60, \"address\": \"Vancouver\", \"year\": 2023, \"month\":"
            + " 5}");
    client().performRequest(request5);
  }

  @Test
  public void testRare() {
    JSONObject result = executeQuery("source = test | rare address");
    verifySchema(result, schema("address", "string"));

    verifyDataRowsInOrder(result, rows("Vancouver"), rows("Portland"), rows("Seattle"));
  }

  // TODO
  @Ignore("when pushdown enabled, the results order changed.")
  public void testRareBy() {
    JSONObject result = executeQuery("source = test | rare address by age");
    verifySchema(result, schema("address", "string"), schema("age", "long"));

    verifyDataRowsInOrder(
        result,
        rows("Vancouver", 60),
        rows("Seattle", 40),
        rows("Portland", 50),
        rows("Seattle", 20),
        rows("Portland", 30));
  }
}
