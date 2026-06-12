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
 * Integration tests for querying wildcard indices where a field has conflicting types (e.g., text
 * vs keyword) across different indices. See GitHub issue #4659.
 */
public class CalciteMixedFieldTypeIT extends PPLIntegTestCase {

  private static final String LOG_TEXT_INDEX = "test_log_text_4659";
  private static final String LOG_KEYWORD_INDEX = "test_log_keyword_4659";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createTestIndices();
  }

  private void createTestIndices() throws IOException {
    // Create index with msg as text type
    if (!isIndexExist(client(), LOG_TEXT_INDEX)) {
      String textMapping =
          "{\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"},"
              + "\"idx\":{\"type\":\"integer\"}}}}";
      createIndexByRestClient(client(), LOG_TEXT_INDEX, textMapping);
      Request bulkReq = new Request("POST", "/" + LOG_TEXT_INDEX + "/_bulk?refresh=true");
      bulkReq.setJsonEntity(
          "{\"index\":{\"_id\":\"1\"}}\n" + "{\"msg\":\"status=200\",\"idx\":1}\n");
      performRequest(client(), bulkReq);
    }

    // Create index with msg as keyword type
    if (!isIndexExist(client(), LOG_KEYWORD_INDEX)) {
      String keywordMapping =
          "{\"mappings\":{\"properties\":{\"msg\":{\"type\":\"keyword\"},"
              + "\"idx\":{\"type\":\"integer\"}}}}";
      createIndexByRestClient(client(), LOG_KEYWORD_INDEX, keywordMapping);
      Request bulkReq = new Request("POST", "/" + LOG_KEYWORD_INDEX + "/_bulk?refresh=true");
      bulkReq.setJsonEntity(
          "{\"index\":{\"_id\":\"1\"}}\n" + "{\"msg\":\"status=200\",\"idx\":2}\n");
      performRequest(client(), bulkReq);
    }
  }

  @Test
  public void testWildcardQueryWithMixedTextAndKeywordField() throws IOException {
    // Query using wildcard index pattern that matches both indices
    // Both documents should be returned regardless of field type conflict
    JSONObject result = executeQuery("source=test_log_*_4659 | fields msg, idx | sort idx");
    verifySchema(result, schema("msg", "string"), schema("idx", "int"));
    verifyDataRows(result, rows("status=200", 1), rows("status=200", 2));
  }

  @Test
  public void testWildcardQueryWithEvalOnMixedField() throws IOException {
    // Eval uses the Calcite script engine to compute expression on each shard.
    // When the merged type is keyword, DOC_VALUE is used, but text shards have no doc_values
    // which returns null and causes the eval to produce null for the text-typed shard.
    JSONObject result =
        executeQuery(
            "source=test_log_*_4659 | eval upper_msg = upper(msg) | fields idx, upper_msg"
                + " | sort idx");
    verifySchema(result, schema("idx", "int"), schema("upper_msg", "string"));
    verifyDataRows(result, rows(1, "STATUS=200"), rows(2, "STATUS=200"));
  }

  @Test
  public void testWildcardQueryWithScriptFilterOnMixedField() throws IOException {
    // Script-based filter pushed down to each shard uses DOC_VALUE retrieval.
    // When the merged type is keyword but the field is text on some shards,
    // doc_values are not available, causing shard failures and missing results.
    JSONObject result =
        executeQuery(
            "source=test_log_*_4659 | where upper(msg) = 'STATUS=200' | fields msg, idx"
                + " | sort idx");
    verifySchema(result, schema("msg", "string"), schema("idx", "int"));
    verifyDataRows(result, rows("status=200", 1), rows("status=200", 2));
  }

  @Test
  public void testWildcardQueryWithRexOnMixedField() throws IOException {
    // Rex command on the conflicting field should work across both indices
    JSONObject result =
        executeQuery(
            "source=test_log_*_4659 | rex field=msg 'status=(?<statusCode>\\\\d+)'"
                + " | fields msg, idx, statusCode | sort idx");
    verifySchema(
        result, schema("msg", "string"), schema("idx", "int"), schema("statusCode", "string"));
    verifyDataRows(result, rows("status=200", 1, "200"), rows("status=200", 2, "200"));
  }
}
