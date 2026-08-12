/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.SearchCommandIT;

public class CalciteSearchCommandIT extends SearchCommandIT {

  private static final String IDX_KEYWORD = "test_5682_keyword";
  private static final String IDX_TEXT = "test_5682_text";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    setupSpecIndices();
  }

  private void setupSpecIndices() throws IOException {
    createSpecIndex(IDX_KEYWORD, "keyword");
    createSpecIndex(IDX_TEXT, "text");
  }

  private void createSpecIndex(String indexName, String fieldType) throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + indexName));
    } catch (ResponseException ignore) {
      // ok
    }

    Request createIndex = new Request("PUT", "/" + indexName);
    createIndex.setJsonEntity(
        "{\n"
            + "  \"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 0},\n"
            + "  \"mappings\": {\n"
            + "    \"properties\": {\n"
            + "      \"name\": {\"type\": \""
            + fieldType
            + "\"}\n"
            + "    }\n"
            + "  }\n"
            + "}");
    client().performRequest(createIndex);

    Request bulk = new Request("POST", "/" + indexName + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\":{}}\n{\"name\":\"foo\"}\n"
            + "{\"index\":{}}\n{\"name\":\"foobar\"}\n"
            + "{\"index\":{}}\n{\"name\":\"food\"}\n"
            + "{\"index\":{}}\n{\"name\":\"FOO\"}\n"
            + "{\"index\":{}}\n{\"name\":\"foo bar\"}\n"
            + "{\"index\":{}}\n{\"name\":\"foo barbaz\"}\n"
            + "{\"index\":{}}\n{\"name\":\"foo-bar\"}\n"
            + "{\"index\":{}}\n{\"name\":\"foo_bar\"}\n"
            + "{\"index\":{}}\n{\"name\":\"foo.bar\"}\n"
            + "{\"index\":{}}\n{\"name\":\"foo/bar\"}\n"
            + "{\"index\":{}}\n{\"name\":\"foo@bar\"}\n");
    client().performRequest(bulk);
  }

  private JSONObject search(String indexName, String predicate) throws IOException {
    // Escape embedded quotes for JSON body wrapping done by executeQuery helper.
    String query =
        "search source=" + indexName + " " + predicate.replace("\"", "\\\"") + " | fields name";
    return executeQuery(query);
  }

  // =============================================================================
  // Group 1 — no special chars, no wildcards
  // =============================================================================

  @Test
  public void testGroup1_1_keyword_foo() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=foo"), 1);
  }

  @Test
  public void testGroup1_1_text_foo() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=foo"), 7);
  }

  @Test
  public void testGroup1_2_keyword_quoted_foo() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo\""), 1);
  }

  @Test
  public void testGroup1_2_text_quoted_foo() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo\""), 7);
  }

  // =============================================================================
  // Group 2 — special chars, no wildcards
  // =============================================================================

  @Test
  public void testGroup2_1_keyword_underscore() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo_bar\""), 1);
  }

  @Test
  public void testGroup2_1_text_underscore() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo_bar\""), 1);
  }

  @Test
  public void testGroup2_2_keyword_dot() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo.bar\""), 1);
  }

  @Test
  public void testGroup2_2_text_dot() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo.bar\""), 1);
  }

  @Test
  public void testGroup2_3_keyword_hyphen() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo-bar\""), 1);
  }

  @Test
  public void testGroup2_3_text_hyphen() throws IOException {
    // Emitted: name:foo\-bar (unquoted, - escaped so parser treats as literal).
    // Analyzer tokenizes to [foo, bar]; boolean OR matches 7 docs whose analyzed tokens
    // contain foo or bar.
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo-bar\""), 7);
  }

  @Test
  public void testGroup2_4_keyword_slash() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo/bar\""), 1);
  }

  @Test
  public void testGroup2_4_text_slash() throws IOException {
    // Emitted: name:foo\/bar (unquoted, / escaped). Analyzer tokenizes to [foo, bar];
    // boolean OR matches 7 docs.
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo/bar\""), 7);
  }

  @Test
  public void testGroup2_5_keyword_at() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo@bar\""), 1);
  }

  @Test
  public void testGroup2_5_text_at() throws IOException {
    // Emitted: name:foo@bar (unquoted; @ is not a Lucene special). Analyzer tokenizes
    // to [foo, bar]; boolean OR matches 7 docs.
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo@bar\""), 7);
  }

  @Test
  public void testGroup2_6_keyword_space() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo bar\""), 1);
  }

  @Test
  public void testGroup2_6_text_space() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo bar\""), 4);
  }

  // =============================================================================
  // Group 3 — trailing wildcard (postfix)
  // =============================================================================

  @Test
  public void testGroup3_1_keyword_unquoted_foostar() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=foo*"), 10);
  }

  @Test
  public void testGroup3_1_text_unquoted_foostar() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=foo*"), 11);
  }

  @Test
  public void testGroup3_2_keyword_quoted_foostar() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo*\""), 10);
  }

  @Test
  public void testGroup3_2_text_quoted_foostar() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo*\""), 11);
  }

  @Test
  public void testGroup3_3_keyword_foo_underscore_star() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo_*\""), 1);
  }

  @Test
  public void testGroup3_3_text_foo_underscore_star() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo_*\""), 1);
  }

  @Test
  public void testGroup3_4_keyword_foo_dot_star() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo.*\""), 1);
  }

  @Test
  public void testGroup3_4_text_foo_dot_star() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo.*\""), 1);
  }

  @Test
  public void testGroup3_5_keyword_foo_hyphen_star() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo-*\""), 1);
  }

  @Test
  public void testGroup3_5_text_foo_hyphen_star() throws IOException {
    // Emitted: name:foo\-* (unquoted, - escaped). This is a WildcardQuery over the analyzed
    // token dictionary — text tokens don't contain '-'. 0 hits (accepted analyzer limit).
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo-*\""), 0);
  }

  @Test
  public void testGroup3_6_keyword_foo_slash_star() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo/*\""), 1);
  }

  @Test
  public void testGroup3_6_text_foo_slash_star() throws IOException {
    // Emitted: name:foo\/* (unquoted, / escaped). WildcardQuery over analyzed tokens;
    // text tokens don't contain '/'. 0 hits (accepted analyzer limit).
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo/*\""), 0);
  }

  @Test
  public void testGroup3_7_keyword_foo_space_barstar() throws IOException {
    // Reported bug row (#5682): keyword whole-value pattern "foo bar*" should match
    // "foo bar" and "foo barbaz".
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo bar*\""), 2);
  }

  @Test
  public void testGroup3_7_text_foo_space_barstar() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo bar*\""), 4);
  }

  // =============================================================================
  // Group 4 — leading wildcard (prefix)
  // =============================================================================

  @Test
  public void testGroup4_1_keyword_starfoo() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"*foo\""), 1);
  }

  @Test
  public void testGroup4_1_text_starfoo() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"*foo\""), 7);
  }

  @Test
  public void testGroup4_2_keyword_starbar() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"*bar\""), 7);
  }

  @Test
  public void testGroup4_2_text_starbar() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"*bar\""), 7);
  }

  @Test
  public void testGroup4_3_keyword_starfoo_bar() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"*foo bar\""), 1);
  }

  @Test
  public void testGroup4_3_text_starfoo_bar() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"*foo bar\""), 4);
  }

  // =============================================================================
  // Group 5 — interior wildcard (* in-between)
  // =============================================================================

  @Test
  public void testGroup5_1_keyword_fstar_r() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"f*r\""), 7);
  }

  @Test
  public void testGroup5_1_text_fstar_r() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"f*r\""), 3);
  }

  @Test
  public void testGroup5_2_keyword_foostar_bar() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo*bar\""), 7);
  }

  @Test
  public void testGroup5_2_text_foostar_bar() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo*bar\""), 3);
  }

  @Test
  public void testGroup5_3_keyword_foo_space_starbaz() throws IOException {
    // Keyword whole-value wildcard: "foo *baz" matches "foo barbaz".
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo *baz\""), 1);
  }

  @Test
  public void testGroup5_3_text_foo_space_starbaz() throws IOException {
    // Corpus-dependent 0 (no adjacent tokens foo→baz analyzed in this fixture).
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo *baz\""), 0);
  }

  @Test
  public void testGroup5_4_keyword_starfoo_barstar() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"*foo bar*\""), 2);
  }

  @Test
  public void testGroup5_4_text_starfoo_barstar() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"*foo bar*\""), 4);
  }

  // =============================================================================
  // Group 6 — ? wildcard (exactly one character)
  // =============================================================================

  @Test
  public void testGroup6_1_keyword_foo_qmark() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo?\""), 1);
  }

  @Test
  public void testGroup6_1_text_foo_qmark() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo?\""), 1);
  }

  @Test
  public void testGroup6_2_keyword_qmark_oo() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"?oo\""), 1);
  }

  @Test
  public void testGroup6_2_text_qmark_oo() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"?oo\""), 7);
  }

  @Test
  public void testGroup6_3_keyword_f_qmark_o() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"f?o\""), 1);
  }

  @Test
  public void testGroup6_3_text_f_qmark_o() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"f?o\""), 7);
  }

  @Test
  public void testGroup6_4_keyword_foo_qmark_bar() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo?bar\""), 6);
  }

  @Test
  public void testGroup6_4_text_foo_qmark_bar() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo?bar\""), 2);
  }

  @Test
  public void testGroup6_5_keyword_foo_space_bqmarkr() throws IOException {
    verifyNumOfRows(search(IDX_KEYWORD, "name=\"foo b?r\""), 1);
  }

  @Test
  public void testGroup6_5_text_foo_space_bqmarkr() throws IOException {
    verifyNumOfRows(search(IDX_TEXT, "name=\"foo b?r\""), 0);
  }
}
