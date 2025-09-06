/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.SQLIntegTestCase.Index.NESTED_WITHOUT_ARRAYS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.hamcrest.Matcher;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteFlattenCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(NESTED_WITHOUT_ARRAYS);
    enableCalcite();
  }

  @Test
  public void testFlattenNestedStruct() throws Exception {
    JSONObject result =
        executeQuery(withSource(TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS, "flatten message"));
    verifySchema(
        result,
        // Nested fields are retrieved as array of nested structs
        // This is because such fields can store either a struct like {"dayOfWeek":1}
        // or an array of structs like [{"dayOfWeek":1}, {"dayOfWeek":2}]
        schema("comment", "array"),
        schema("myNum", "bigint"),
        schema("someField", "string"),
        // Nested fields are retrieved as array of nested structs
        schema("message", "array"),
        schema("author", "string"),
        schema("dayOfWeek", "bigint"),
        schema("info", "string"));
    verifyDataRows(result, getExpectedRows());
  }

  @Test
  public void testFlattenWithAliases() throws Exception {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | flatten message as (creator, dow, information)",
                TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS));
    verifySchema(
        result,
        schema("comment", "array"),
        schema("myNum", "bigint"),
        schema("someField", "string"),
        schema("message", "array"),
        schema("creator", "string"),
        schema("dow", "bigint"),
        schema("information", "string"));
    verifyDataRows(result, getExpectedRows());
  }

  @Test
  public void testFlattenWithMismatchedNumberOfAliasesShouldThrow() {
    Throwable t =
        expectThrows(
            Exception.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | flatten message as a, b, c, d",
                        TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS)));
    verifyErrorMessageContains(
        t,
        "The number of aliases has to match the number of flattened fields. Expected 3"
            + " (message.author, message.dayOfWeek, message.info), got 4 (a, b, c, d)");
  }

  @Test
  public void testFlattenNullField() throws IOException {
    final int docId = 6;
    Request insertRequest =
        new Request(
            "PUT",
            String.format(
                "/%s/_doc/%d?refresh=true", TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS, docId));
    insertRequest.setJsonEntity(
        "{\"message\": null,\"comment\":null,\"myNum\":0,\"someField\":\"\"}\n");
    client().performRequest(insertRequest);

    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where someField='' | flatten message as (creator, dow, information)",
                TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS));
    verifySchema(
        result,
        schema("comment", "array"),
        schema("myNum", "bigint"),
        schema("someField", "string"),
        schema("message", "array"),
        schema("creator", "string"),
        schema("dow", "bigint"),
        schema("information", "string"));
    verifyDataRows(result, rows(null, 0, "", null, null, null, null));

    Request deleteRequest =
        new Request(
            "DELETE",
            String.format(
                "/%s/_doc/%d?refresh=true", TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS, docId));
    client().performRequest(deleteRequest);
  }

  // TODO: Enable after fixing issue #3459 and #3751
  //  https://github.com/opensearch-project/sql/issues/3459
  //  https://github.com/opensearch-project/sql/issues/3751
  @Ignore
  @Test
  public void testFlattenAfterFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where myNum=1 | fields message | flatten message",
                TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS));
    verifySchema(
        result,
        schema("message", "array"),
        schema("author", "string"),
        schema("dayOfWeek", "bigint"),
        schema("info", "string"));
    verifyDataRows(
        result,
        rows(
            new JSONArray()
                .put(new JSONObject().put("info", "a").put("author", "e").put("dayOfWeek", 1)),
            "e",
            1,
            "a"));
  }

  @SuppressWarnings("unchecked")
  private static Matcher<JSONArray>[] getExpectedRows() {
    return new org.hamcrest.TypeSafeMatcher[] {
      rows(
          new JSONArray().put(new JSONObject().put("data", "ab").put("likes", 3)),
          1,
          "b",
          new JSONArray()
              .put(new JSONObject().put("info", "a").put("author", "e").put("dayOfWeek", 1)),
          "e",
          1,
          "a"),
      rows(
          new JSONArray().put(new JSONObject().put("data", "aa").put("likes", 2)),
          2,
          "a",
          new JSONArray()
              .put(new JSONObject().put("info", "b").put("author", "f").put("dayOfWeek", 2)),
          "f",
          2,
          "b"),
      rows(
          new JSONArray().put(new JSONObject().put("data", "aa").put("likes", 3)),
          3,
          "a",
          new JSONArray()
              .put(new JSONObject().put("info", "c").put("author", "g").put("dayOfWeek", 1)),
          "g",
          1,
          "c"),
      rows(
          new JSONArray().put(new JSONObject().put("data", "ab").put("likes", 1)),
          4,
          "b",
          new JSONArray()
              .put(new JSONObject().put("info", "c").put("author", "h").put("dayOfWeek", 4)),
          "h",
          4,
          "c"),
      rows(
          new JSONArray().put(new JSONObject().put("data", "bb").put("likes", 10)),
          3,
          "a",
          new JSONArray()
              .put(new JSONObject().put("info", "zz").put("author", "zz").put("dayOfWeek", 6)),
          "zz",
          6,
          "zz")
    };
  }
}
