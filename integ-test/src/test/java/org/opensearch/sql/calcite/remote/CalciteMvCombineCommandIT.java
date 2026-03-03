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

    verifySchema(
        result,
        schema("ip", null, "string"),
        schema("bytes", null, "bigint"),
        schema("tags", null, "string"),
        schema("packets_str", null, "array"));

    verifyDataRows(result, rows("10.0.0.1", 100, "t1", List.of("10", "20", "30")));
  }

  @Test
  public void testMvCombine_singleRowGroupStaysSingleRow() throws IOException {
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

    verifySchema(
        result,
        schema("ip", null, "string"),
        schema("bytes", null, "bigint"),
        schema("tags", null, "string"),
        schema("packets_str", null, "array"));

    verifyDataRows(result, rows("10.0.0.3", 300, "t3", List.of("5")));
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

    JSONObject result = executeQuery(base + " | mvcombine packets_str | sort ip");

    verifyNumOfRows(result, 2);

    verifySchema(
        result,
        schema("ip", null, "string"),
        schema("bytes", null, "bigint"),
        schema("tags", null, "string"),
        schema("packets_str", null, "array"));

    // MV contents differ per group → helper cannot express membership safely
    JSONArray r0 = result.getJSONArray("datarows").getJSONArray(0);
    JSONArray r1 = result.getJSONArray("datarows").getJSONArray(1);

    List<String> mv0 = toStringListDropNulls(r0.get(3));
    List<String> mv1 = toStringListDropNulls(r1.get(3));

    Assertions.assertEquals("10.0.0.7", r0.getString(0));
    Assertions.assertEquals("10.0.0.8", r1.getString(0));

    Assertions.assertTrue(mv0.containsAll(List.of("1", "2")));
    Assertions.assertEquals(2, mv0.size());
    Assertions.assertEquals(List.of("9"), mv1);
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

    String q = base + " | mvcombine delim='|' packets_str";

    try {
      JSONObject result = executeQuery(q);

      verifyNumOfRows(result, 1);

      verifySchema(
          result,
          schema("ip", null, "string"),
          schema("bytes", null, "bigint"),
          schema("tags", null, "string"),
          schema("packets_str", null, "array"));

      Object cell = result.getJSONArray("datarows").getJSONArray(0).get(3);
      Assertions.assertTrue(cell instanceof JSONArray);

      List<String> mv = toStringListDropNulls(cell);
      Assertions.assertTrue(mv.contains("1"));
      Assertions.assertTrue(mv.contains("2"));
      Assertions.assertTrue(mv.contains("3"));
    } catch (ResponseException e) {
      Assertions.assertTrue(isSyntaxBadRequest(e));
    }
  }

  // ---------------------------
  // Edge case / error semantics
  // ---------------------------

  @Test
  public void testMvCombine_missingField_shouldReturn4xx() throws IOException {
    ResponseException ex =
        Assertions.assertThrows(
            ResponseException.class,
            () -> executeQuery("source=" + INDEX + " | mvcombine does_not_exist"));

    int status = ex.getResponse().getStatusLine().getStatusCode();

    Assertions.assertEquals(400, status, "Unexpected status. ex=" + ex.getMessage());

    String msg = ex.getMessage();
    Assertions.assertTrue(msg.contains("Field [does_not_exist] not found."), msg);
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
