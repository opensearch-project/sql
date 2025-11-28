/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CASCADED_NESTED;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DEEP_NESTED;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.ppl.WhereCommandIT;

public class CalciteWhereCommandIT extends WhereCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.NESTED_SIMPLE);
    loadIndex(Index.DEEP_NESTED);
    loadIndex(Index.CASCADED_NESTED);
  }

  @Override
  protected String getIncompatibleTypeErrMsg() {
    return "In expression types are incompatible: fields type LONG, values type [INTEGER, INTEGER,"
        + " STRING]";
  }

  @Test
  public void testFilterOnComputedNestedFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval proj_name_len=length(projects.name) | fields projects.name,"
                    + " proj_name_len | where proj_name_len > 29",
                TEST_INDEX_DEEP_NESTED));
    verifySchema(result, schema("projects.name", "string"), schema("proj_name_len", "int"));
    verifyDataRows(result, rows("AWS Redshift Spectrum querying", 30));
  }

  @Test
  public void testFilterOnNestedAndRootFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where city.name = 'Seattle' and length(projects.name) > 29 | fields"
                    + " city.name, projects.name",
                TEST_INDEX_DEEP_NESTED));
    verifySchema(result, schema("city.name", "string"), schema("projects.name", "string"));
    verifyDataRows(result, rows("Seattle", "AWS Redshift Spectrum querying"));
  }

  @Test
  public void testFilterOnNestedFields() throws IOException {
    // address is a nested object
    JSONObject result1 =
        executeQuery(
            String.format(
                "source=%s | where address.city = 'New york city' | fields address.city",
                TEST_INDEX_NESTED_SIMPLE));
    verifySchema(result1, schema("address.city", "string"));
    verifyDataRows(result1, rows("New york city"));

    JSONObject result2 =
        executeQuery(
            String.format(
                "source=%s | where address.city in ('Miami', 'san diego') | fields address.city",
                TEST_INDEX_NESTED_SIMPLE));
    verifyDataRows(result2, rows("Miami"), rows("san diego"));
  }

  @Test
  public void testFilterOnMultipleCascadedNestedFields() throws IOException {
    // SQL's static type system does not allow returning list[int] in place of int
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where author.books.reviews.rating >=4 and author.books.reviews.rating"
                    + " < 6 and author.books.title = 'The Shining' | fields author.books",
                TEST_INDEX_CASCADED_NESTED));
    verifySchema(result, schema("author.books", "array"));
    verifyDataRows(
        result,
        rows(
            List.of(
                Map.of(
                    "title",
                    "The Shining",
                    "reviews",
                    List.of(
                        Map.of(
                            "review_date",
                            "2022-09-03",
                            "rating",
                            3,
                            "comment",
                            "Brilliant but terrifying"),
                        Map.of(
                            "review_date",
                            "2023-04-12",
                            "rating",
                            4,
                            "comment",
                            "Psychological horror at its best"),
                        Map.of(
                            "review_date",
                            "2023-10-28",
                            "rating",
                            2,
                            "comment",
                            "Too slow in places"))))));
  }

  @Test
  public void testScriptFilterOnDifferentNestedHierarchyShouldThrow() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    Throwable t =
        assertThrows(
            Exception.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | where author.books.reviews.rating + length(author.books.title)"
                            + " > 10",
                        TEST_INDEX_CASCADED_NESTED)));
    verifyErrorMessageContains(
        t,
        "Accessing multiple nested fields under different hierarchies in script is not supported:"
            + " [author.books.reviews, author.books]");
  }

  @Test
  public void testAggFilterOnNestedFields() throws IOException {
    JSONObject result =
        executeQuery(
            StringUtils.format(
                "source=%s | stats count(eval(author.name < 'K')) as george_and_jk",
                TEST_INDEX_CASCADED_NESTED));
    verifySchema(result, schema("george_and_jk", "bigint"));
    verifyDataRows(result, rows(2));
  }
}
