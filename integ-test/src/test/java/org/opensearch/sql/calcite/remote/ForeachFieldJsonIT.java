/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.junit.Assert.assertThrows;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Foreach collection modes over index fields (Splunk-parity scenarios). */
public class ForeachFieldJsonIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    if (!TestUtils.isIndexExist(client(), "test_foreach_field2")) {
      String mapping =
          "{\"mappings\":{\"properties\":{\"jsonfield\":{\"type\":\"keyword\"},"
              + "\"jsonstrs\":{\"type\":\"keyword\"},\"nativenums\":{\"type\":\"long\"},"
              + "\"nested_objs\":{\"type\":\"nested\",\"properties\":{\"a\":{\"type\":\"long\"}}}}}}";
      TestUtils.createIndexByRestClient(client(), "test_foreach_field2", mapping);

      Request r = new Request("PUT", "/test_foreach_field2/_doc/1?refresh=true");
      r.setJsonEntity(
          "{\"jsonfield\": \"[10,20,30]\", \"jsonstrs\": \"[\\\"a\\\",\\\"b\\\"]\","
              + " \"nativenums\": [1, 2, 3], \"nested_objs\": [{\"a\": 1}, {\"a\": 2}]}");
      client().performRequest(r);
    }
  }

  @Test
  public void testJsonArrayModeOnFieldWithNumericContent() throws IOException {
    // Splunk: field holding "[10,20,30]" with foreach mode=json_array sums to 60.
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval total = 0 | foreach mode=json_array jsonfield ["
                + " eval total = total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "double"));
    verifyDataRows(result, rows(60.0));
  }

  @Test
  public void testJsonArrayModeOnFieldWithStringContent() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval r = '' | foreach mode=json_array jsonstrs ["
                + " eval r = concat(r, <<ITEM>>) ] | fields r");
    verifySchema(result, schema("r", "string"));
    verifyDataRows(result, rows("ab"));
  }

  /**
   * Native OpenSearch array fields (a long field holding [1,2,3]) are typed as scalar BIGINT at
   * plan time because OpenSearch mappings do not distinguish scalars from arrays. foreach
   * multivalue mode therefore rejects them; documents the current known limitation rather than the
   * desired behavior.
   */
  @Test
  public void testNativeArrayFieldIsRejected() {
    assertThrows(
        ResponseException.class,
        () ->
            executeQuery(
                "source=test_foreach_field2 | eval total = 0 | foreach mode=multivalue nativenums"
                    + " [ eval total = total + <<ITEM>> ] | fields total"));
  }

  /**
   * Nested-typed fields map to ARRAY&lt;ANY&gt; at plan time, so multivalue mode iterates them. The
   * lambda here only counts elements; it does not dereference the object item.
   */
  @Test
  public void testNestedFieldMultivalueIterates() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval total = 0 | foreach mode=multivalue nested_objs ["
                + " eval total = total + 1 ] | fields total");
    verifySchema(result, schema("total", "int"));
    verifyDataRows(result, rows(2));
  }

  /**
   * Cross-feed behavior differs from Splunk by design. Splunk silently no-ops when a mode is fed
   * the wrong collection shape; we either still work (json_array mode on a real array iterates it -
   * more permissive) or fail loudly at plan time (multivalue mode on a JSON-text field).
   */
  @Test
  public void testJsonArrayModeOnRealArrayIterates() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval nums = array(1, 2, 3), total = 0 | foreach"
                + " mode=json_array nums [ eval total = total + <<ITEM>> ] | fields total | head"
                + " 1");
    verifySchema(result, schema("total", "double"));
    verifyDataRows(result, rows(6.0));
  }

  @Test
  public void testMultivalueModeOnJsonTextFieldIsRejected() {
    assertThrows(
        ResponseException.class,
        () ->
            executeQuery(
                "source=test_foreach_field2 | eval total = 0 | foreach mode=multivalue jsonfield"
                    + " [ eval total = total + <<ITEM>> ] | fields total"));
  }
}
