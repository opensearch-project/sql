/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
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
  // Happy paths (core mvcombine)
  // ---------------------------

  @Test
  public void testMvCombine_basicGroupCollapsesToOneRow() throws IOException {
    // Dataset: ip=10.0.0.1 bytes=100 tags=t1 packets_str in [10,20,30]
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
    // Dataset: ip=10.0.0.2 bytes=200 tags=t2 packets_str=7
    String q =
        "source="
            + INDEX
            + " | where ip='10.0.0.2' and bytes=200 and tags='t2'"
            + " | fields ip, bytes, tags, packets_str"
            + " | mvcombine packets_str";

    JSONObject result = executeQuery(q);
    verifyNumOfRows(result, 1);

    JSONArray row = result.getJSONArray("datarows").getJSONArray(0);
    List<String> mv = toStringListDropNulls(row.get(3));

    Assertions.assertEquals(1, mv.size(), "Expected single-value MV, got " + mv);
    Assertions.assertEquals("7", mv.get(0));
  }

  @Test
  public void testMvCombine_missingTargetWithinGroup_collapses_nonNullPreserved()
      throws IOException {
    // Dataset: ip=10.0.0.3 bytes=300 tags=t3 has:
    // - one doc with packets_str=5
    // - one doc missing packets_str
    // - one doc with letters only
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

    // Requirements we can enforce safely:
    // - collapse happens into 1 row
    // - non-null value is preserved
    Assertions.assertTrue(mv.contains("5"), "Expected packets_str to include 5, got " + mv);
  }

  // ---------------------------
  // Multi-group behavior
  // ---------------------------

  @Test
  public void testMvCombine_multipleGroups_producesOneRowPerGroupKey() throws IOException {
    // Dataset has:
    // ip=10.0.0.7 bytes=700 tags=t7 packets_str=[1,2]
    // ip=10.0.0.8 bytes=700 tags=t7 packets_str=[9]
    String base =
        "source="
            + INDEX
            + " | where (ip='10.0.0.7' or ip='10.0.0.8') and bytes=700 and tags='t7'"
            + " | fields ip, bytes, tags, packets_str";

    // precondition (should exist; if someone edits the dataset, fail with a crisp message)
    JSONObject before = executeQuery(base);
    int beforeRows = before.getJSONArray("datarows").length();
    Assertions.assertTrue(beforeRows >= 1, "Expected dataset rows for multi-group test, got 0");

    JSONObject result = executeQuery(base + " | mvcombine packets_str | sort ip");
    int outRows = result.getJSONArray("datarows").length();
    Assertions.assertEquals(
        2, outRows, "Expected 2 groups (10.0.0.7 and 10.0.0.8), got " + outRows);

    // Spot-check values without assuming ordering within MV arrays
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
  // delim/nomv: happy paths + edge tolerance
  // ---------------------------

  @Test
  public void testMvCombine_nomv_defaultDelim_ifSupported_elseSyntaxRejected() throws Exception {
    // Uses ip=10.0.0.9 bytes=900 tags=t9 packets_str=[1,2,3]
    String base =
        "source="
            + INDEX
            + " | where ip='10.0.0.9' and bytes=900 and tags='t9'"
            + " | fields ip, bytes, tags, packets_str";

    // If supported: should return a scalar string containing 1,2,3 in some delimiter format.
    // If unsupported: expected 400 syntax rejection.
    String q = base + " | mvcombine packets_str nomv";

    try {
      JSONObject result = executeQuery(q);
      verifyNumOfRows(result, 1);

      Object cell = result.getJSONArray("datarows").getJSONArray(0).get(3);
      Assertions.assertTrue(
          cell instanceof String, "Expected nomv output scalar string, got: " + cell);

      String s = (String) cell;
      Assertions.assertTrue(
          Pattern.compile("1.*2.*3").matcher(s).find(),
          "Expected nomv string to contain values 1,2,3 in order, got: " + s);
    } catch (ResponseException e) {
      Assertions.assertTrue(
          isSyntaxBadRequest(e),
          "Expected syntax rejection if nomv unsupported, got: " + e.getMessage());
    }
  }

  @Test
  public void testMvCombine_nomvWithCustomDelim_ifSupported_elseSyntaxRejected() throws Exception {
    // IMPORTANT: single quotes in query
    String base =
        "source="
            + INDEX
            + " | where ip='10.0.0.9' and bytes=900 and tags='t9'"
            + " | fields ip, bytes, tags, packets_str";

    // Test both parameter orders
    String q1 = base + " | mvcombine packets_str nomv delim='|'";
    String q2 = base + " | mvcombine packets_str delim='|' nomv";

    JSONObject result;
    try {
      result = executeQuery(q1);
    } catch (ResponseException e1) {
      if (!isSyntaxBadRequest(e1)) throw e1;

      try {
        result = executeQuery(q2);
      } catch (ResponseException e2) {
        Assertions.assertTrue(
            isSyntaxBadRequest(e2),
            "Expected syntax rejection for unsupported nomv/delim, got: " + e2.getMessage());
        return; // unsupported -> acceptable
      }
    }

    // Supported -> validate scalar string output and delimiter presence
    verifyNumOfRows(result, 1);
    Object cell = result.getJSONArray("datarows").getJSONArray(0).get(3);
    Assertions.assertTrue(
        cell instanceof String, "Expected nomv output scalar string, got: " + cell);

    String s = (String) cell;
    Assertions.assertTrue(s.contains("|"), "Expected delimiter '|' in: " + s);
    Assertions.assertTrue(
        Pattern.compile("1\\|.*2\\|.*3|1.*\\|.*2.*\\|.*3|1.*2.*3").matcher(s).find(),
        "Expected values to be present in the joined output, got: " + s);
  }

  @Test
  public void testMvCombine_delimWithoutNomv_shouldNotChangeMvShape_ifSupported_elseSyntaxRejected()
      throws Exception {
    // If delim is only meaningful with nomv, then:
    // - supported behavior should still return multivalue (JSONArray) for the field
    // - or it may reject delim when nomv absent (400)
    String base =
        "source="
            + INDEX
            + " | where ip='10.0.0.9' and bytes=900 and tags='t9'"
            + " | fields ip, bytes, tags, packets_str";

    String q = base + " | mvcombine packets_str delim='|'";

    try {
      JSONObject result = executeQuery(q);
      verifyNumOfRows(result, 1);

      Object cell = result.getJSONArray("datarows").getJSONArray(0).get(3);
      Assertions.assertTrue(
          cell instanceof JSONArray,
          "Expected multivalue array (delim without nomv should not coerce to string), got: "
              + cell);
    } catch (ResponseException e) {
      Assertions.assertTrue(
          isSyntaxBadRequest(e),
          "Expected syntax rejection if delim-only unsupported, got: " + e.getMessage());
    }
  }

  // ---------------------------
  // Edge / error semantics
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

    // Keep it broad: different layers throw different messages
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
