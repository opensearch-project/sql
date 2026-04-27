/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for NOT LIKE with null/missing field values. Tests the fix for issue #5169: NOT
 * LIKE should exclude rows where the field is null or missing.
 */
public class CalciteNotLikeNullIT extends PPLIntegTestCase {

  private static final String TEST_INDEX = "issue5169_not_like_null";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createTestIndex();
  }

  private void createTestIndex() throws IOException {
    try {
      Request deleteIndex = new Request("DELETE", "/" + TEST_INDEX);
      client().performRequest(deleteIndex);
    } catch (ResponseException e) {
      // Index doesn't exist, which is fine
    }

    Request createIndex = new Request("PUT", "/" + TEST_INDEX);
    createIndex.setJsonEntity(
        "{\n"
            + "  \"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"keyword_field\": {\"type\": \"keyword\"},\n"
            + "      \"int_field\": {\"type\": \"integer\"}\n"
            + "    }\n"
            + "  }\n"
            + "}");
    client().performRequest(createIndex);

    Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=true");
    bulkRequest.setJsonEntity(
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"keyword_field\": \"hello\", \"int_field\": 1}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"keyword_field\": \"world\", \"int_field\": 2}\n"
            + "{\"index\":{\"_id\":\"3\"}}\n"
            + "{\"keyword_field\": \"\", \"int_field\": 3}\n"
            + "{\"index\":{\"_id\":\"4\"}}\n"
            + "{\"keyword_field\": \"special chars...\", \"int_field\": 4}\n"
            + "{\"index\":{\"_id\":\"5\"}}\n"
            + "{\"keyword_field\": null, \"int_field\": null}\n");
    client().performRequest(bulkRequest);
  }

  @Test
  public void testNotLikeExcludesNull() throws IOException {
    // NOT LIKE '%ello%' should match 'world', '', 'special chars...' but NOT null
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX
                + " | where NOT keyword_field LIKE '%ello%'"
                + " | sort keyword_field"
                + " | fields keyword_field");
    verifyDataRowsInOrder(result, rows(""), rows("special chars..."), rows("world"));
  }

  @Test
  public void testNotLikeWithNoMatch() throws IOException {
    // NOT LIKE '%zzz%' should return all non-null rows (4 rows)
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX
                + " | where NOT keyword_field LIKE '%zzz%'"
                + " | sort keyword_field"
                + " | fields keyword_field");
    verifyDataRowsInOrder(result, rows(""), rows("hello"), rows("special chars..."), rows("world"));
  }

  @Test
  public void testNotGreaterThanExcludesNull() throws IOException {
    // NOT int_field > 2 should return rows with int_field 1, 2 but NOT null
    JSONObject result =
        executeQuery(
            "source="
                + TEST_INDEX
                + " | where NOT int_field > 2"
                + " | sort int_field"
                + " | fields int_field");
    verifyDataRowsInOrder(result, rows(1), rows(2));
  }
}
