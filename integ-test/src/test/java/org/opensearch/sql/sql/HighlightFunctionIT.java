/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestsConstants;

public class HighlightFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.BEER);
  }

  @Test
  public void single_highlight_test() {
    String query = "SELECT Tags, highlight('Tags') FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));

    verifySchema(
        response, schema("Tags", null, "text"), schema("highlight('Tags')", null, "nested"));
    assertEquals(1, response.getInt("total"));

    verifyDataRows(
        response,
        rows(
            "alcohol-level yeast home-brew champagne",
            new JSONArray(List.of("alcohol-level <em>yeast</em> home-brew champagne"))));
  }

  @Test
  public void highlight_optional_arguments_test() {
    String query =
        "SELECT highlight('Tags', pre_tags='<mark>', post_tags='</mark>') "
            + "FROM %s WHERE match(Tags, 'yeast') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));

    verifySchema(
        response,
        schema("highlight('Tags', pre_tags='<mark>', post_tags='</mark>')", null, "nested"));

    assertEquals(1, response.getInt("total"));

    verifyDataRows(
        response,
        rows(new JSONArray(List.of("alcohol-level <mark>yeast</mark> home-brew champagne"))));
  }

  @Test
  public void highlight_multiple_optional_arguments_test() {
    String query =
        "SELECT highlight(Title), highlight(Body, pre_tags='<mark style=\\\"background-color:"
            + " green;\\\">', post_tags='</mark>') FROM %s WHERE multi_match([Title, Body], 'IPA')"
            + " LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));

    verifySchema(
        response,
        schema("highlight(Title)", null, "nested"),
        schema(
            "highlight(Body, pre_tags='<mark style=\"background-color: green;\">', "
                + "post_tags='</mark>')",
            null,
            "nested"));

    assertEquals(1, response.getInt("size"));

    verifyDataRows(
        response,
        rows(
            new JSONArray(
                List.of("What are the differences between an <em>IPA</em>" + " and its variants?")),
            new JSONArray(
                List.of(
                    "<p>I know what makes an <mark style=\"background-color: green;\">IPA</mark> an"
                        + " <mark style=\"background-color: green;\">IPA</mark>, but what are the"
                        + " unique characteristics of it's common variants?",
                    "To be specific, the ones I'm interested in are Double <mark"
                        + " style=\"background-color: green;\">IPA</mark> and Black <mark"
                        + " style=\"background-color: green;\">IPA</mark>, but general differences"
                        + " between"))));
  }

  @Test
  public void multiple_highlight_test() {
    String query =
        "SELECT highlight(Title), highlight(Tags) FROM %s WHERE MULTI_MATCH([Title, Tags], 'hops')"
            + " LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(
        response,
        schema("highlight(Title)", null, "nested"),
        schema("highlight(Tags)", null, "nested"));
    assertEquals(1, response.getInt("total"));

    verifyDataRows(
        response,
        rows(
            new JSONArray(List.of("What uses do <em>hops</em> have outside of brewing?")),
            new JSONArray(List.of("<em>hops</em> history"))));
  }

  @Test
  public void wildcard_highlight_test() {
    String query =
        "SELECT highlight('*itle') FROM %s WHERE MULTI_MATCH([Title, Tags], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));

    verifySchema(response, schema("highlight('*itle')", null, "object"));
    assertEquals(1, response.getInt("total"));

    verifyDataRows(
        response,
        rows(
            new JSONObject(
                ImmutableMap.of(
                    "Title",
                    new JSONArray(
                        List.of("What uses do <em>hops</em> have outside of brewing?"))))));
  }

  @Test
  public void wildcard_multi_field_highlight_test() {
    String query =
        "SELECT highlight('T*') FROM %s WHERE MULTI_MATCH([Title, Tags], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));

    verifySchema(response, schema("highlight('T*')", null, "object"));
    assertEquals(1, response.getInt("total"));

    verifyDataRows(
        response,
        rows(
            new JSONObject(
                ImmutableMap.of(
                    "Title",
                        new JSONArray(
                            List.of("What uses do <em>hops</em> have outside of brewing?")),
                    "Tags", new JSONArray(List.of("<em>hops</em> history"))))));
  }

  @Test
  public void highlight_all_test() {
    String query = "SELECT highlight('*') FROM %s WHERE MULTI_MATCH([Title, Tags], 'hops') LIMIT 1";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));

    verifySchema(response, schema("highlight('*')", null, "object"));
    assertEquals(1, response.getInt("total"));

    verifyDataRows(
        response,
        rows(
            new JSONObject(
                ImmutableMap.of(
                    "Title",
                        new JSONArray(
                            List.of("What uses do <em>hops</em> have outside of brewing?")),
                    "Tags", new JSONArray(List.of("<em>hops</em> history"))))));
  }

  @Test
  public void highlight_no_limit_test() {
    String query = "SELECT highlight(Body) FROM %s WHERE MATCH(Body, 'hops')";
    JSONObject response = executeJdbcRequest(String.format(query, TestsConstants.TEST_INDEX_BEER));
    verifySchema(response, schema("highlight(Body)", null, "nested"));
    assertEquals(2, response.getInt("total"));

    verifyDataRows(
        response,
        rows(
            new JSONArray(
                List.of(
                    "Boiling affects <em>hops</em>, by boiling"
                        + " off the aroma and extracting more of the organic acids that provide"))),
        rows(
            new JSONArray(
                List.of(
                    "<p>Do <em>hops</em> have (or had in the past) any use outside of brewing"
                        + " beer?",
                    "when-was-the-first-beer-ever-brewed\">dating first modern beers</a> we have"
                        + " the first record of cultivating <em>hops</em>",
                    "predating the first record of use of <em>hops</em> in beer by nearly a"
                        + " century.",
                    "Could the <em>hops</em> have been cultivated for any other purpose than"
                        + " brewing, or can we safely assume if they"))));
  }
}
