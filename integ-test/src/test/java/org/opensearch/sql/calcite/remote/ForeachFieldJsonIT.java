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

/** Foreach collection modes over index fields. */
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
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval total = 0 | foreach mode=json_array jsonfield ["
                + " eval total = total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "double"));
    verifyDataRows(result, rows(60.0));
  }

  @Test
  public void testJsonArrayFieldInfersNumericItemForAbs() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval result = 0 | foreach mode=json_array jsonfield ["
                + " eval result = abs(<<ITEM>>) ] | fields result");
    verifySchema(result, schema("result", "double"));
    verifyDataRows(result, rows(30.0));
  }

  @Test
  public void testJsonArrayFieldInfersNumericItemForComparison() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval count = 0 | foreach mode=json_array jsonfield ["
                + " eval count = count + if(<<ITEM>> > 9, 1, 0) ] | fields count");
    verifySchema(result, schema("count", "int"));
    verifyDataRows(result, rows(3));
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

  @Test
  public void testJsonArrayFieldSafelyCoercesNonNumericItems() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval total = 0 | foreach mode=json_array jsonstrs ["
                + " eval total = total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "double"));
    verifyDataRows(result, rows((Object) null));
  }

  /**
   * Native OpenSearch array fields (a long field holding [1,2,3]) are typed as scalar BIGINT at
   * plan time because OpenSearch mappings do not distinguish scalars from arrays. foreach
   * multivalue mode therefore rejects them; documents the current known limitation rather than the
   * desired behavior.
   */
  @Test
  public void testNativeArrayFieldMappingMismatchIsNoOp() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval total = 0 | foreach mode=multivalue nativenums"
                + " [ eval total = total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "int"));
    verifyDataRows(result, rows(0));
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

  /** Collection modes are no-ops when the input has a different collection shape. */
  @Test
  public void testJsonArrayModeOnRealArrayIsNoOp() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval nums = array(1, 2, 3), total = 0 | foreach"
                + " mode=json_array nums [ eval total = total + <<ITEM>> ] | fields total | head"
                + " 1");
    verifySchema(result, schema("total", "int"));
    verifyDataRows(result, rows(0));
  }

  @Test
  public void testMultivalueModeOnJsonTextFieldIsNoOp() throws IOException {
    JSONObject result =
        executeQuery(
            "source=test_foreach_field2 | eval total = 0 | foreach mode=multivalue jsonfield"
                + " [ eval total = total + <<ITEM>> ] | fields total");
    verifySchema(result, schema("total", "int"));
    verifyDataRows(result, rows(0));
  }
}
