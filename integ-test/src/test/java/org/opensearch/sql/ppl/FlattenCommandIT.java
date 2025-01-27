/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_MULTI_NESTED_TYPE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_WITH_NULLS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class FlattenCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.NESTED_WITHOUT_ARRAYS);
    loadIndex(Index.NESTED_WITH_NULLS);
    loadIndex(Index.MULTI_NESTED);
  }

  @Test
  public void testFlattenStructBasic() throws IOException {
    String query =
        String.format(
            "source=%s | flatten message | fields info, author, dayOfWeek",
            TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("info", "string"),
        schema("author", "string"),
        schema("dayOfWeek", "integer"));
    verifyDataRows(
        result,
        rows("a", "e", 1),
        rows("b", "f", 2),
        rows("c", "g", 1),
        rows("c", "h", 4),
        rows("zz", "zz", 6));
  }

  @Test
  public void testFlattenStructMultiple() throws IOException {
    String query =
        String.format(
            "source=%s | flatten message | flatten comment "
                + "| fields info, author, dayOfWeek, data, likes",
            TEST_INDEX_NESTED_TYPE_WITHOUT_ARRAYS);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("info", "string"),
        schema("author", "string"),
        schema("dayOfWeek", "integer"),
        schema("data", "string"),
        schema("likes", "integer"));
    verifyDataRows(
        result,
        rows("a", "e", 1, "ab", 1),
        rows("b", "f", 2, "aa", 2),
        rows("c", "g", 1, "aa", 3),
        rows("c", "h", 4, "ab", 1),
        rows("zz", "zz", 6, "bb", 10));
  }

  @Test
  public void testFlattenStructNull() throws IOException {
    String query =
        String.format(
            "source=%s | flatten message | fields info, author, dayOfWeek",
            TEST_INDEX_NESTED_WITH_NULLS);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("info", "string"),
        schema("author", "string"),
        schema("dayOfWeek", "integer"));
    verifyDataRows(
        result,
        rows("e", null, 5),
        rows("c", "g", 1),
        rows("c", "h", 4),
        rows("zz", "zz", 6),
        rows("zz", "z\"z", 6),
        rows(null, "e", 7),
        rows("a", "e", 1),
        rows("rr", "this \"value\" contains quotes", 3),
        rows(null, null, null),
        rows(null, null, null));
  }

  @Test
  public void testFlattenStructNested() throws IOException {
    String query =
        String.format(
            "source=%s | flatten message | fields info, name, street, number, dayOfWeek",
            TEST_INDEX_MULTI_NESTED_TYPE);
    JSONObject result = executeQuery(query);

    verifySchema(
        result,
        schema("info", "string"),
        schema("name", "string"),
        schema("street", "string"),
        schema("number", "integer"),
        schema("dayOfWeek", "integer"));
    verifyDataRows(
        result,
        rows("a", "e", "bc", 1, 1),
        rows("b", "f", "ab", 2, 2),
        rows("c", "g", "sk", 3, 1),
        rows("d", "h", "mb", 4, 4),
        rows("zz", "yy", "qc", 6, 6));
  }
}
