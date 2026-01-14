/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteMvCombineCommandIT extends PPLIntegTestCase {

  private static final String INDEX = Index.MVCOMBINE.getName();

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.MVCOMBINE);
  }

  // ---------------------------
  // Sanity (precondition)
  // ---------------------------

  @Test
  public void testSanity_datasetIsLoaded() throws IOException {
    JSONObject result = executeQuery("source=" + INDEX + " | head 5");
    int rows = result.getJSONArray("datarows").length();
    Assertions.assertTrue(rows > 0, "Expected MVCOMBINE dataset to have rows, got 0");
  }

  // ---------------------------
  // Happy path (core mvcombine)
  // ---------------------------

  @Test
  public void testMvCombine_basicGroupCollapsesToOneRow() throws IOException {
    String q =
        "source="
            + INDEX
            + " | where ip='10.0.0.1' and bytes=100 and tags='t1'"
            + " | fields ip, bytes, tags, packets_str"
            + " | mvcombine packets_str";

    JSONObject result = executeQuery(q);
    verifyNumOfRows(result, 1);

    JSONArray row = result.getJSONArray("datarows").getJSONArray(0);
    Assertions.assertEquals("10.0.0.1", row.getString(0));
    Assertions.assertEquals("100", String.valueOf(row.get(1)));
    Assertions.assertEquals("t1", row.getString(2));

    List<String> mv = toStringListDropNulls(row.get(3));
    Assertions.assertTrue(mv.contains("10"), "Expected packets_str to include 10, got " + mv);
    Assertions.assertTrue(mv.contains("20"), "Expected packets_str to include 20, got " + mv);
    Assertions.assertTrue(mv.contains("30"), "Expected packets_str to include 30, got " + mv);
  }

  @Test
  public void testMvCombine_singleRowGroupStaysSingleRow() throws IOException {
    // NOTE: Keep output minimal + deterministic to safely verify schema + datarows
    String q =
        "source="
            + INDEX
            + " | where ip='10.0.0.2' and bytes=200 and tags='t2'"
            + " | fields ip, tags, packets_str"
            + " | mvcombine packets_str";

    JSONObject result = executeQuery(q);

    verifySchema(
        result,
        schema("ip", null, "string"),
        schema("tags", null, "string"),
        schema("packets_str", null, "array"));

    verifyDataRows(result, rows("10.0.0.2", "t2", new JSONArray().put("7")));
  }

  @Test
  public void testMvCombine_missingTargetWithinGroup_collapses_nonNullPreserved()
      throws IOException {
    String q =
        "source="
            + INDEX
            + " | where ip='10.0.0.3' and bytes=300 and tags='t3'"
            + " | fields ip, bytes, tags, packets_str"
            + " | mvcombine packets_str";

    JSONObject result = executeQuery(q);
    verifyNumOfRows(result, 1);

    JSONArray row = result.getJSONArray("datarows").getJSONArray(0);
    List<String> mv = toStringListKeepNulls(row.get(3));

    Assertions.assertTrue(mv.contains("5"), "Expected packets_str to include 5, got " + mv);
  }

  // ---------------------------
  // Multi-group behavior
  // ---------------------------

  @Test
  public void testMvCombine_multipleGroups_producesOneRowPerGroupKey() throws IOException {
    String base =
        "source="
            + INDEX
            + " | where (ip='10.0.0.7' or ip='10.0.0.8') and bytes=700 and tags='t7'"
            + " | fields ip, bytes, tags, packets_str";

    JSONObject before = executeQuery(base);
    int beforeRows = before.getJSONArray("datarows").length();
    Assertions.assertTrue(beforeRows >= 1, "Expected dataset rows for multi-group test, got 0");

    JSONObject result = executeQuery(base + " | mvcombine packets_str | sort ip");
    int outRows = result.getJSONArray("datarows").length();
    Assertions.assertEquals(
        2, outRows, "Expected 2 groups (10.0.0.7 and 10.0.0.8), got " + outRows);

    JSONArray r0 = result.getJSONArray("datarows").getJSONArray(0);
    JSONArray r1 = result.getJSONArray("datarows").getJSONArray(1);

    String ip0 = r0.getString(0);
    String ip1 = r1.getString(0);

    if ("10.0.0.7".equals(ip0)) {
      List<String> mv0 = toStringListDropNulls(r0.get(3));
      Assertions.assertTrue(
          mv0.contains("1") && mv0.contains("2"),
          "Expected 10.0.0.7 to include 1 and 2, got " + mv0);

      List<String> mv1 = toStringListDropNulls(r1.get(3));
      Assertions.assertTrue(mv1.contains("9"), "Expected 10.0.0.8 to include 9, got " + mv1);
    } else {
      List<String> mv0 = toStringListDropNulls(r0.get(3));
      Assertions.assertTrue(mv0.contains("9"), "Expected 10.0.0.8 to include 9, got " + mv0);

      List<String> mv1 = toStringListDropNulls(r1.get(3));
      Assertions.assertTrue(
          mv1.contains("1") && mv1.contains("2"),
          "Expected 10.0.0.7 to include 1 and 2, got " + mv1);
    }
  }

  // ---------------------------
  // delim: Splunk-compatible command input + output shape
  // ---------------------------

  @Test
  public void testMvCombine_delim_shouldNotChangeMvShape_ifSupported_elseSyntaxRejected()
      throws Exception {
    String base =
        "source="
            + INDEX
            + " | where ip='10.0.0.9' and bytes=900 and tags='t9'"
            + " | fields ip, bytes, tags, packets_str";

    // Splunk-style: options before the field
    String q = base + " | mvcombine delim='|' packets_str";

    try {
      JSONObject result = executeQuery(q);
      verifyNumOfRows(result, 1);

      Object cell = result.getJSONArray("datarows").getJSONArray(0).get(3);
      Assertions.assertTrue(
          cell instanceof JSONArray,
          "Expected multivalue array (delim should not coerce to string), got: " + cell);

      // Optional sanity: values exist (order not guaranteed)
      List<String> mv = toStringListDropNulls(cell);
      Assertions.assertTrue(mv.contains("1"), "Expected MV to include 1, got: " + mv);
      Assertions.assertTrue(mv.contains("2"), "Expected MV to include 2, got: " + mv);
      Assertions.assertTrue(mv.contains("3"), "Expected MV to include 3, got: " + mv);
    } catch (ResponseException e) {
      Assertions.assertTrue(
          isSyntaxBadRequest(e),
          "Expected syntax rejection if delim unsupported, got: " + e.getMessage());
    }
  }

  // ---------------------------
  // Edge case / error semantics
  // ---------------------------

  @Test
  public void testMvCombine_missingField_shouldReturn4xx() throws IOException {
    try {
      executeQuery("source=" + INDEX + " | mvcombine does_not_exist");
      Assertions.fail("Expected ResponseException was not thrown");
    } catch (ResponseException e) {
      int status = e.getResponse().getStatusLine().getStatusCode();
      Assertions.assertTrue(
          status >= 400 && status < 500,
          "Expected 4xx for missing field, got " + status + " msg=" + e.getMessage());
    }
  }

  // ---------------------------
  // Helpers
  // ---------------------------

  private static boolean isSyntaxBadRequest(ResponseException e) {
    int status = e.getResponse().getStatusLine().getStatusCode();
    if (status != 400) return false;

    String msg = e.getMessage();
    if (msg == null) return false;

    return msg.contains("SyntaxCheckException")
        || msg.contains("Invalid Query")
        || msg.contains("parsing_exception")
        || msg.contains("ParseException");
  }

  /** JSONArray -> list (nulls preserved), scalar -> singleton list, null -> empty list. */
  private static List<String> toStringListKeepNulls(Object cell) {
    if (cell == null || cell == JSONObject.NULL) {
      return Collections.emptyList();
    }
    if (cell instanceof JSONArray arr) {
      List<String> out = new ArrayList<>();
      for (int i = 0; i < arr.length(); i++) {
        Object v = arr.get(i);
        out.add(v == JSONObject.NULL ? null : String.valueOf(v));
      }
      return out;
    }
    return List.of(String.valueOf(cell));
  }

  /** Same as above but drops null entries. */
  private static List<String> toStringListDropNulls(Object cell) {
    List<String> all = toStringListKeepNulls(cell);
    if (all.isEmpty()) return all;

    List<String> out = new ArrayList<>();
    for (String v : all) {
      if (v != null) out.add(v);
    }
    return out;
  }
}
