/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * A dedup gives each output column the value of its field, with pushdown on (the top_hits rows are
 * mapped to the output columns) and off.
 */
public class CalcitePPLDedupFieldMappingIT extends PPLIntegTestCase {

  private static final String INDEX = "ppl_dedup_field_mapping";

  private static final String MAPPING =
      "{\"mappings\": {\"properties\": {\"g\": {\"type\": \"keyword\"},"
          + " \"a\": {\"type\": \"keyword\"}, \"b\": {\"type\": \"keyword\"},"
          + " \"k\": {\"type\": \"keyword\"}, \"n\": {\"type\": \"integer\"},"
          + " \"ka\": {\"type\": \"alias\", \"path\": \"k\"},"
          + " \"obj\": {\"properties\": {\"name\": {\"type\": \"keyword\"}}},"
          + " \"ts\": {\"type\": \"date\"}, \"tm\": {\"type\": \"date\"},"
          + " \"d1\": {\"type\": \"date\", \"format\": \"yyyy/MM/dd HH:mm:ss\"},"
          + " \"d2\": {\"type\": \"date\", \"format\": \"epoch_second\"},"
          + " \"d3\": {\"type\": \"date\", \"format\": \"yyyy-MM-dd\"},"
          + " \"tags\": {\"type\": \"keyword\"}, \"one\": {\"type\": \"keyword\"},"
          + " \"objs\": {\"properties\": {\"a\": {\"type\": \"keyword\"}}}}}}";

  private static final String DOC =
      "{\"g\": \"x\", \"a\": \"A\", \"b\": \"B\", \"k\": \"K\", \"n\": 1,"
          + " \"obj\": {\"name\": \"o\"}, \"ts\": \"2024-01-01\", \"tm\": 1704067200000,"
          + " \"d1\": \"2024/01/02 03:04:05\", \"d2\": 1704164645, \"d3\": \"2024-01-03\","
          + " \"tags\": [\"t1\", \"t2\"], \"one\": [\"u1\"],"
          + " \"objs\": [{\"a\": \"A1\"}, {\"a\": \"A2\"}]}";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    if (!isIndexExist(client(), INDEX)) {
      createIndexByRestClient(client(), INDEX, MAPPING);
      Request request = new Request("POST", "/" + INDEX + "/_bulk?refresh=true");
      request.setJsonEntity("{\"index\": {\"_id\": \"1\"}}\n" + DOC + "\n");
      performRequest(client(), request);
    }
  }

  private JSONObject query(String commands) throws IOException {
    String query = String.format("source=%s | %s", INDEX, commands);
    if (!isPushdownDisabled()) {
      // The dedup is pushed down, so the rows are those of the top_hits mapping.
      assertThat(explainQueryToString(query), containsString("top_hits"));
    }
    return executeQuery(query);
  }

  @Test
  public void testAliasAndItsField() throws IOException {
    verifyDataRows(query("dedup g | fields g, k, ka"), rows("x", "K", "K"));
  }

  @Test
  public void testEvalCopyAndItsField() throws IOException {
    verifyDataRows(query("eval n2 = n | dedup g | fields g, n, n2"), rows("x", 1, 1));
  }

  @Test
  public void testColumnsThatSwapFields() throws IOException {
    verifyDataRows(
        query("rename a as t, b as a, t as b | dedup g | fields g, a, b"), rows("x", "B", "A"));
  }

  @Test
  public void testCopyAndRedefinedField() throws IOException {
    verifyDataRows(
        query("eval c = b, b = a | dedup g | fields g, a, b, c"), rows("x", "A", "A", "B"));
  }

  /** A script column named like a field that a renamed column reads is returned apart. */
  @Test
  public void testScriptColumnNamedLikeARenamedField() throws IOException {
    verifyDataRows(
        query("rename k as j | eval k = 1 | dedup g | fields g, j, k"), rows("x", "K", 1));
  }

  /** The script name the top_hits gives k is none of the other columns' names. */
  @Test
  public void testScriptColumnNamedLikeTheRenamedScript() throws IOException {
    verifyDataRows(
        query("rename k as j | eval k = 1, `k#script` = 2 | dedup g | fields g, j, k, `k#script`"),
        rows("x", "K", 1, 2));
  }

  /** With a script column, a hit has no _source unless asked for, and the object reads from it. */
  @Test
  public void testObjectBesideAScriptColumn() throws IOException {
    verifyDataRows(
        query("eval x = 1 | dedup g | fields g, obj, x"),
        rows("x", new JSONObject("{\"name\": \"o\"}").toMap(), 1));
  }

  /** A column named like another field reads with the type of the field it reads. */
  @Test
  public void testCopyNamedLikeAnotherField() throws IOException {
    verifyDataRows(query("eval n = k | dedup g | fields g, n"), rows("x", "K"));
  }

  @Test
  public void testColumnsThatSwapFieldsOfDifferentTypes() throws IOException {
    verifyDataRows(
        query("rename k as t, n as k, t as n | dedup g | fields g, k, n"), rows("x", 1, "K"));
  }

  /** A date read under a name outside the index reads as a date. */
  @Test
  public void testRenamedDate() throws IOException {
    verifyDataRows(
        query("rename ts as t2 | dedup g | fields g, t2"), rows("x", "2024-01-01 00:00:00"));
  }

  /**
   * A date read under the name of a date field of another format reads with the format of the field
   * it reads: tm holds epoch milliseconds, d1 a custom format and d2 epoch seconds.
   */
  @Test
  public void testDateNamedLikeADateOfAnotherFormat() throws IOException {
    verifyDataRows(
        query("eval d2 = tm | dedup g | fields g, d2"), rows("x", "2024-01-01 00:00:00"));
    verifyDataRows(
        query("eval ts = d1 | dedup g | fields g, ts"), rows("x", "2024-01-02 03:04:05"));
    verifyDataRows(
        query("eval ts = d2 | dedup g | fields g, ts"), rows("x", "2024-01-02 03:04:05"));
  }

  /** A renamed group key reads the bucket key, which the custom date-only format cannot parse. */
  @Test
  public void testRenamedGroupKeyOfACustomDateFormat() throws IOException {
    verifyDataRows(query("rename d3 as dd | dedup dd | fields dd, g"), rows("2024-01-03", "x"));
  }

  /** A field of several values keeps them all, and an array of one value stays an array. */
  @Test
  public void testFieldOfSeveralValues() throws IOException {
    verifyDataRows(
        query("dedup g | fields g, tags, one"), rows("x", List.of("t1", "t2"), List.of("u1")));
  }

  /**
   * A field inside an array of objects reads the first object's, as a search hit's row reads it.
   */
  @Test
  public void testFieldInsideAnArrayOfObjects() throws IOException {
    verifyDataRows(query("dedup g | fields g, objs.a"), rows("x", "A1"));
    // With a script column, the hit has _source only if the request asks for it.
    verifyDataRows(query("eval x = 1 | dedup g | fields g, objs.a, x"), rows("x", "A1", 1));
  }

  /** Without field type tolerance, a field of several values reads its first, as a search hit. */
  @Test
  public void testFieldsWithoutFieldTypeTolerance() throws IOException {
    updateClusterSettings(
        new ClusterSetting("transient", Settings.Key.FIELD_TYPE_TOLERANCE.getKeyValue(), "false"));
    try {
      verifyDataRows(query("dedup g | fields g, tags, one, objs.a"), rows("x", "t1", "u1", "A1"));
    } finally {
      updateClusterSettings(
          new ClusterSetting("transient", Settings.Key.FIELD_TYPE_TOLERANCE.getKeyValue(), null));
    }
  }
}
