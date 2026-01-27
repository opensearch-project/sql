/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Base class for Calcite PPL spath command integration tests. Provides common utility methods for
 * working with JSON data.
 */
public abstract class CalcitePPLSpathTestBase extends PPLIntegTestCase {
  protected static final String INDEX_JSON = "test_json";
  protected static final String INDEX_WITHOUT_JSON = "test_regular";
  protected static final String INDEX_JSON_CONFLICT = "test_json_conflict";

  protected void putJsonItem(int id, String category, String json) throws Exception {
    Request request = new Request("PUT", String.format("/%s/_doc/%d?refresh=true", INDEX_JSON, id));
    request.setJsonEntity(docWithJson(category, json));
    client().performRequest(request);
  }

  protected void putJsonItemConflict(int id, String category, String json) throws Exception {
    Request request =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", INDEX_JSON_CONFLICT, id));
    request.setJsonEntity(docWithJson(category, json));
    client().performRequest(request);
  }

  protected void putRegularItem(int id, String userId, String orderId, double amount, String notes)
      throws Exception {
    Request request =
        new Request("PUT", String.format("/%s/_doc/%d?refresh=true", INDEX_WITHOUT_JSON, id));
    request.setJsonEntity(regularDoc(userId, orderId, amount, notes));
    client().performRequest(request);
  }

  protected String regularDoc(String userId, String orderId, double amount, String notes) {
    return String.format(
        java.util.Locale.ROOT,
        sj("{'userId': '%s', 'orderId': '%s', 'amount': %.1f, 'notes': '%s'}"),
        userId,
        orderId,
        amount,
        notes);
  }

  protected String docWithJson(String category, String json) {
    return String.format(sj("{'category': '%s', 'userData': '%s'}"), category, escape(json));
  }

  protected String escape(String json) {
    return json.replace("\"", "\\\"");
  }

  /** Converts single-quote JSON to double-quote JSON. */
  protected String sj(String singleQuoteJson) {
    return singleQuoteJson.replace("'", "\"");
  }
}
