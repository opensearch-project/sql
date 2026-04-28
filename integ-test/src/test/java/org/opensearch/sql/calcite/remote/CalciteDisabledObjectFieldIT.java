/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for querying inner fields of an object that is declared with {@code "enabled":
 * false} in the index mapping. Such objects are stored in {@code _source} but are not indexed, so
 * the plugin cannot learn the shape of the object from the mapping. See GitHub issue #4906.
 */
public class CalciteDisabledObjectFieldIT extends PPLIntegTestCase {

  private static final String DISABLED_OBJECT_INDEX = "test_disabled_object_4906";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createTestIndex();
  }

  private void createTestIndex() throws IOException {
    if (!isIndexExist(client(), DISABLED_OBJECT_INDEX)) {
      String mapping =
          "{\"mappings\":{\"properties\":{"
              + "\"log\":{\"type\":\"object\",\"enabled\":false}"
              + "}}}";
      createIndexByRestClient(client(), DISABLED_OBJECT_INDEX, mapping);
      Request bulkReq = new Request("POST", "/" + DISABLED_OBJECT_INDEX + "/_bulk?refresh=true");
      bulkReq.setJsonEntity(
          "{\"index\":{\"_id\":\"1\"}}\n" + "{\"log\":{\"a\":1,\"c\":{\"d\":2}}}\n");
      performRequest(client(), bulkReq);
    }
  }

  @Test
  public void testSelectNestedFieldFromDisabledObject() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields log.c.d", DISABLED_OBJECT_INDEX));
    // Schema is undefined because disabled object fields have no field mapping.
    // The important check is that the value is extracted correctly from _source.
    verifyDataRows(result, rows(2));
  }

  @Test
  public void testSelectTopLevelFieldFromDisabledObject() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields log.a", DISABLED_OBJECT_INDEX));
    // Schema is undefined because disabled object fields have no field mapping.
    // The important check is that the value is extracted correctly from _source.
    verifyDataRows(result, rows(1));
  }

  @Test
  public void testSelectEntireDisabledObject() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields log", DISABLED_OBJECT_INDEX));
    verifySchema(result, schema("log", "struct"));
  }

  @Test
  public void testSelectIntermediateFieldFromDisabledObject() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | fields log.c", DISABLED_OBJECT_INDEX));
    verifySchema(result, schema("log.c", "struct"));
  }
}
