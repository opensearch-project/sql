/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class MultiMatchIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.BEER);
  }

  /*
  The 'beer.stackexchange' index is a dump of beer.stackexchange.com converted to the format which might be ingested by OpenSearch.
  This is a forum like StackOverflow with questions about beer brewing. The dump contains both questions, answers and comments.
  The reference query is:
    select count(Id) from beer.stackexchange where multi_match(["Tags" ^ 1.5, Title, `Body` 4.2], 'taste') and Tags like '% % %' and Title like '%';
  It filters out empty `Tags` and `Title`.
  */

  @Test
  public void test_mandatory_params() throws IOException {
    String query =
        "SELECT Id FROM "
            + TEST_INDEX_BEER
            + " WHERE multi_match([\\\"Tags\\\" ^ 1.5, Title, 'Body' 4.2], 'taste')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(16, result.getInt("total"));
  }

  @Test
  public void test_all_params() {
    String query =
        "SELECT Id FROM "
            + TEST_INDEX_BEER
            + " WHERE multi_match(['Body', Tags], 'taste beer', operator='and',"
            + " analyzer=english,auto_generate_synonyms_phrase_query=true, boost = 0.77,"
            + " cutoff_frequency=0.33,fuzziness = 'AUTO:1,5', fuzzy_transpositions = false, lenient"
            + " = true, max_expansions = 25,minimum_should_match = '2<-25% 9<-3', prefix_length ="
            + " 7, tie_breaker = 0.3,type = most_fields, slop = 2, zero_terms_query = 'ALL');";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(10, result.getInt("total"));
  }

  @Test
  public void verify_wildcard_test() {
    String query1 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE multi_match(['Tags'], 'taste')";
    JSONObject result1 = executeJdbcRequest(query1);
    String query2 = "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE multi_match(['T*'], 'taste')";
    JSONObject result2 = executeJdbcRequest(query2);
    assertNotEquals(result2.getInt("total"), result1.getInt("total"));

    String query =
        "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE multi_match(['*Date'], '2014-01-22');";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(10, result.getInt("total"));
  }

  @Test
  public void test_multimatch_alternate_parameter_syntax() {
    String query =
        "SELECT Tags FROM "
            + TEST_INDEX_BEER
            + " WHERE multimatch('query'='taste', 'fields'='Tags')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(8, result.getInt("total"));
  }

  @Test
  public void test_multimatchquery_alternate_parameter_syntax() {
    String query =
        "SELECT Tags FROM "
            + TEST_INDEX_BEER
            + " WHERE multimatchquery(query='cicerone', fields='Tags')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    verifyDataRows(result, rows("serving cicerone restaurants"), rows("taste cicerone"));
  }

  @Test
  public void test_quoted_multi_match_alternate_parameter_syntax() {
    String query =
        "SELECT Tags FROM "
            + TEST_INDEX_BEER
            + " WHERE multi_match('query'='cicerone', 'fields'='Tags')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    verifyDataRows(result, rows("serving cicerone restaurants"), rows("taste cicerone"));
  }

  @Test
  public void test_multi_match_alternate_parameter_syntax() {
    String query =
        "SELECT Tags FROM "
            + TEST_INDEX_BEER
            + " WHERE multi_match(query='cicerone', fields='Tags')";
    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
    verifyDataRows(result, rows("serving cicerone restaurants"), rows("taste cicerone"));
  }

  @Test
  public void test_wildcard_multi_match_alternate_parameter_syntax() {
    String query =
        "SELECT Body FROM "
            + TEST_INDEX_BEER
            + " WHERE multi_match(query='IPA', fields='B*') LIMIT 1";
    JSONObject result = executeJdbcRequest(query);
    verifyDataRows(
        result,
        rows(
            "<p>I know what makes an IPA an IPA, but what are the unique characteristics of it's"
                + " common variants? To be specific, the ones I'm interested in are Double IPA and"
                + " Black IPA, but general differences between any other styles would be welcome"
                + " too. </p>\n"));
  }

  @Test
  public void test_all_params_multimatchquery_alternate_parameter_syntax() {
    String query =
        "SELECT Id FROM "
            + TEST_INDEX_BEER
            + " WHERE multimatchquery(query='cicerone', fields='Tags', 'operator'='or',"
            + " analyzer=english,auto_generate_synonyms_phrase_query=true, boost = 0.77,"
            + " cutoff_frequency=0.33,fuzziness = 'AUTO:1,5', fuzzy_transpositions = false, lenient"
            + " = true, max_expansions = 25,minimum_should_match = '2<-25% 9<-3', prefix_length ="
            + " 7, tie_breaker = 0.3,type = most_fields, slop = 2, zero_terms_query = 'ALL');";

    JSONObject result = executeJdbcRequest(query);
    assertEquals(2, result.getInt("total"));
  }

  @Test
  public void multi_match_alternate_syntax() throws IOException {
    String query =
        "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE CreationDate = multi_match('2014-01-22');";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(8, result.getInt("total"));
  }

  @Test
  public void multimatch_alternate_syntax() throws IOException {
    String query =
        "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE CreationDate = multimatch('2014-01-22');";
    var result = new JSONObject(executeQuery(query, "jdbc"));
    assertEquals(8, result.getInt("total"));
  }

  @Test
  public void multi_match_alternate_syntaxes_return_the_same_results() throws IOException {
    String query1 =
        "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE multi_match(['CreationDate'], '2014-01-22');";
    String query2 =
        "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE CreationDate = multi_match('2014-01-22');";
    String query3 =
        "SELECT Id FROM " + TEST_INDEX_BEER + " WHERE CreationDate = multimatch('2014-01-22');";
    var result1 = new JSONObject(executeQuery(query1, "jdbc"));
    var result2 = new JSONObject(executeQuery(query2, "jdbc"));
    var result3 = new JSONObject(executeQuery(query3, "jdbc"));
    assertEquals(result1.getInt("total"), result2.getInt("total"));
    assertEquals(result1.getInt("total"), result3.getInt("total"));
  }
}
