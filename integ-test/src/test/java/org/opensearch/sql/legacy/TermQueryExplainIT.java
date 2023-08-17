/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

public class TermQueryExplainIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.ONLINE);
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_TWO);
    loadIndex(Index.DOG);
    loadIndex(Index.DOGS2);
    loadIndex(Index.DOGS3);
    loadIndex(Index.EMPLOYEE_NESTED);
  }

  @Test
  public void testNonExistingIndex() throws IOException {
    try {
      explainQuery(
          "SELECT firstname, lastname "
              + "FROM opensearch_sql_test_fake_index "
              + "WHERE firstname = 'Leo'");
      Assert.fail("Expected ResponseException, but none was thrown");

    } catch (ResponseException e) {
      assertThat(
          e.getResponse().getStatusLine().getStatusCode(),
          equalTo(RestStatus.BAD_REQUEST.getStatus()));
      final String entity = TestUtils.getResponseBody(e.getResponse());
      assertThat(entity, containsString("no such index"));
      assertThat(entity, containsString("\"type\": \"IndexNotFoundException\""));
    }
  }

  @Test
  public void testNonResolvingIndexPattern() throws IOException {
    try {
      explainQuery("SELECT * FROM opensearch_sql_test_blah_blah* WHERE firstname = 'Leo'");
      Assert.fail("Expected ResponseException, but none was thrown");

    } catch (ResponseException e) {
      assertThat(
          e.getResponse().getStatusLine().getStatusCode(),
          equalTo(RestStatus.BAD_REQUEST.getStatus()));
      final String entity = TestUtils.getResponseBody(e.getResponse());
      assertThat(entity, containsString("Field [firstname] cannot be found or used here."));
      assertThat(entity, containsString("\"type\": \"SemanticAnalysisException\""));
    }
  }

  @Test
  public void testNonResolvingIndexPatternWithExistingIndex() throws IOException {
    String result =
        explainQuery(
            "SELECT * "
                + "FROM opensearch_sql_test_blah_blah*, opensearch-sql_test_index_bank "
                + "WHERE state = 'DC'");
    assertThat(result, containsString("\"term\":{\"state.keyword\""));
  }

  @Test
  public void testNonResolvingIndexPatternWithNonExistingIndex() throws IOException {
    try {
      explainQuery(
          "SELECT firstname, lastname "
              + "FROM opensearch_sql_test_blah_blah*, another_fake_index "
              + "WHERE firstname = 'Leo'");
      Assert.fail("Expected ResponseException, but none was thrown");
    } catch (ResponseException e) {
      assertThat(
          e.getResponse().getStatusLine().getStatusCode(),
          equalTo(RestStatus.BAD_REQUEST.getStatus()));
      final String entity = TestUtils.getResponseBody(e.getResponse());
      assertThat(entity, containsString("no such index"));
      assertThat(entity, containsString("\"type\": \"IndexNotFoundException\""));
    }
  }

  @Test
  public void testNonCompatibleMappings() throws IOException {
    try {
      explainQuery("SELECT * FROM opensearch-sql_test_index_dog, opensearch-sql_test_index_dog2");
      Assert.fail("Expected ResponseException, but none was thrown");
    } catch (ResponseException e) {
      assertThat(
          e.getResponse().getStatusLine().getStatusCode(),
          equalTo(RestStatus.BAD_REQUEST.getStatus()));
      final String entity = TestUtils.getResponseBody(e.getResponse());
      assertThat(entity, containsString("Field [holdersName] have conflict type"));
      assertThat(entity, containsString("\"type\": \"SemanticAnalysisException\""));
    }
  }

  /**
   * The dog_name field has same type in dog and dog2 index. But, the holdersName field has
   * different type.
   */
  @Test
  public void testNonCompatibleMappingsButTheFieldIsNotUsed() throws IOException {
    String result =
        explainQuery(
            "SELECT dog_name FROM opensearch-sql_test_index_dog, opensearch-sql_test_index_dog2"
                + " WHERE dog_name = 'dog'");
    System.out.println(result);
    assertThat(result, containsString("dog_name"));
    assertThat(result, containsString("_source"));
  }

  @Test
  public void testEqualFieldMappings() throws IOException {
    String result =
        explainQuery(
            "SELECT color "
                + "FROM opensearch-sql_test_index_dog2, opensearch-sql_test_index_dog3");
    assertThat(result, containsString("color"));
    assertThat(result, containsString("_source"));
  }

  @Test
  public void testIdenticalMappings() throws IOException {
    String result =
        explainQuery(
            "SELECT firstname, birthdate, state "
                + "FROM opensearch-sql_test_index_bank, opensearch-sql_test_index_bank_two "
                + "WHERE state = 'WA' OR male = true");
    assertThat(result, containsString("term"));
    assertThat(result, containsString("state.keyword"));
    assertThat(result, containsString("_source"));
  }

  @Test
  public void testIdenticalMappingsWithTypes() throws IOException {
    String result =
        explainQuery(
            "SELECT firstname, birthdate, state FROM opensearch-sql_test_index_bank/account,"
                + " opensearch-sql_test_index_bank_two/account_two WHERE state = 'WA' OR male ="
                + " true");
    assertThat(result, containsString("term"));
    assertThat(result, containsString("state.keyword"));
    assertThat(result, containsString("_source"));
  }

  @Test
  public void testIdenticalMappingsWithPartialType() throws IOException {
    String result =
        explainQuery(
            "SELECT firstname, birthdate, state "
                + "FROM opensearch-sql_test_index_bank/account, opensearch-sql_test_index_bank_two "
                + "WHERE state = 'WA' OR male = true");
    assertThat(result, containsString("term"));
    assertThat(result, containsString("state.keyword"));
    assertThat(result, containsString("_source"));
  }

  @Test
  public void testTextFieldOnly() throws IOException {

    String result =
        explainQuery(
            "SELECT firstname, birthdate, state "
                + "FROM opensearch-sql_test_index_bank "
                + "WHERE firstname = 'Abbas'");
    assertThat(result, containsString("term"));
    assertThat(result, not(containsString("firstname.")));
  }

  @Test
  public void testTextAndKeywordAppendsKeywordAlias() throws IOException {
    String result =
        explainQuery(
            "SELECT firstname, birthdate, state "
                + "FROM opensearch-sql_test_index_bank "
                + "WHERE state = 'WA' OR lastname = 'Chen'");
    assertThat(result, containsString("term"));
    assertThat(result, containsString("state.keyword"));
    assertThat(result, not(containsString("lastname.")));
  }

  @Test
  public void testBooleanFieldNoKeywordAlias() throws IOException {

    String result = explainQuery("SELECT * FROM opensearch-sql_test_index_bank WHERE male = false");
    assertThat(result, containsString("term"));
    assertThat(result, not(containsString("male.")));
  }

  @Test
  public void testDateFieldNoKeywordAlias() throws IOException {

    String result =
        explainQuery("SELECT * FROM opensearch-sql_test_index_bank WHERE birthdate = '2018-08-19'");
    assertThat(result, containsString("term"));
    assertThat(result, not(containsString("birthdate.")));
  }

  @Test
  public void testNumberNoKeywordAlias() throws IOException {
    String result = explainQuery("SELECT * FROM opensearch-sql_test_index_bank WHERE age = 32");
    assertThat(result, containsString("term"));
    assertThat(result, not(containsString("age.")));
  }

  @Test
  public void inTestInWhere() throws IOException {
    String result =
        explainQuery(
            "SELECT * "
                + "FROM opensearch-sql_test_index_bank "
                + "WHERE state IN ('WA' , 'PA' , 'TN')");
    assertThat(result, containsString("term"));
    assertThat(result, containsString("state.keyword"));
  }

  @Test
  @Ignore // TODO: enable when subqueries are fixed
  public void inTestInWhereSubquery() throws IOException {
    String result =
        explainQuery(
            "SELECT * FROM opensearch-sql_test_index_bank/account WHERE state IN (SELECT state FROM"
                + " opensearch-sql_test_index_bank WHERE city = 'Nicholson')");
    assertThat(result, containsString("term"));
    assertThat(result, containsString("state.keyword"));
  }

  @Test
  public void testKeywordAliasGroupBy() throws IOException {
    String result =
        explainQuery(
            "SELECT firstname, state "
                + "FROM opensearch-sql_test_index_bank/account "
                + "GROUP BY firstname, state");
    assertThat(result, containsString("term"));
    assertThat(result, containsString("state.keyword"));
  }

  @Test
  public void testKeywordAliasGroupByUsingTableAlias() throws IOException {
    String result =
        explainQuery(
            "SELECT a.firstname, a.state "
                + "FROM opensearch-sql_test_index_bank/account a "
                + "GROUP BY a.firstname, a.state");
    assertThat(result, containsString("term"));
    assertThat(result, containsString("state.keyword"));
  }

  @Test
  public void testKeywordAliasOrderBy() throws IOException {
    String result =
        explainQuery("SELECT * FROM opensearch-sql_test_index_bank ORDER BY state, lastname ");
    assertThat(result, containsString("\"state.keyword\":{\"order\":\"asc\""));
    assertThat(result, containsString("\"lastname\":{\"order\":\"asc\"}"));
  }

  @Test
  public void testKeywordAliasOrderByUsingTableAlias() throws IOException {
    String result =
        explainQuery(
            "SELECT * "
                + "FROM opensearch-sql_test_index_bank b "
                + "ORDER BY b.state, b.lastname ");
    assertThat(result, containsString("\"state.keyword\":{\"order\":\"asc\""));
    assertThat(result, containsString("\"lastname\":{\"order\":\"asc\"}"));
  }

  @Test
  @Ignore // TODO: verify the returned query is correct and fix the expected output
  public void testJoinWhere() throws IOException {
    String expectedOutput =
        TestUtils.fileToString("src/test/resources/expectedOutput/term_join_where", true);
    String result =
        explainQuery(
            "SELECT a.firstname, a.lastname , b.city "
                + "FROM opensearch-sql_test_index_account a "
                + "JOIN opensearch-sql_test_index_account b "
                + "ON a.city = b.city "
                + "WHERE a.city IN ('Nicholson', 'Yardville')");

    assertThat(result.replaceAll("\\s+", ""), equalTo(expectedOutput.replaceAll("\\s+", "")));
  }

  @Test
  public void testJoinAliasMissing() throws IOException {
    try {
      explainQuery(
          "SELECT a.firstname, a.lastname , b.city "
              + "FROM opensearch-sql_test_index_account a "
              + "JOIN opensearch-sql_test_index_account b "
              + "ON a.city = b.city "
              + "WHERE city IN ('Nicholson', 'Yardville')");
      Assert.fail("Expected ResponseException, but none was thrown");
    } catch (ResponseException e) {
      assertThat(
          e.getResponse().getStatusLine().getStatusCode(),
          equalTo(RestStatus.BAD_REQUEST.getStatus()));
      final String entity = TestUtils.getResponseBody(e.getResponse());
      assertThat(entity, containsString("Field name [city] is ambiguous"));
      assertThat(entity, containsString("\"type\": \"VerificationException\""));
    }
  }

  @Test
  public void testNestedSingleConditionAllFields() throws IOException {
    String result =
        explainQuery(
            "SELECT * "
                + "FROM opensearch-sql_test_index_employee_nested e, e.projects p "
                + "WHERE p.name = 'something' ");
    assertThat(
        result, containsString("\"term\":{\"projects.name.keyword\":{\"value\":\"something\""));
    assertThat(result, containsString("\"path\":\"projects\""));
  }

  @Test
  public void testNestedMultipleCondition() throws IOException {
    String result =
        explainQuery(
            "SELECT e.id, p.name "
                + "FROM opensearch-sql_test_index_employee_nested e, e.projects p "
                + "WHERE p.name = 'something' and p.started_year = 1990 ");
    assertThat(
        result, containsString("\"term\":{\"projects.name.keyword\":{\"value\":\"something\""));
    assertThat(result, containsString("\"term\":{\"projects.started_year\":{\"value\":1990"));
    assertThat(result, containsString("\"path\":\"projects\""));
  }

  @Test
  public void testConditionsOnDifferentNestedDocs() throws IOException {
    String result =
        explainQuery(
            "SELECT p.name, c.likes  "
                + "FROM opensearch-sql_test_index_employee_nested e, e.projects p, e.comments c "
                + "WHERE p.name = 'something' or c.likes = 56 ");
    assertThat(
        result, containsString("\"term\":{\"projects.name.keyword\":{\"value\":\"something\""));
    assertThat(result, containsString("\"term\":{\"comments.likes\":{\"value\":56"));
    assertThat(result, containsString("\"path\":\"projects\""));
    assertThat(result, containsString("\"path\":\"comments\""));
  }

  @Test
  public void testNestedSingleConditionSpecificFields() throws IOException {
    String result =
        explainQuery(
            "SELECT e.id, p.name "
                + "FROM opensearch-sql_test_index_employee_nested e, e.projects p "
                + "WHERE p.name = 'hello' or p.name = 'world' ");
    assertThat(result, containsString("\"term\":{\"projects.name.keyword\":{\"value\":\"hello\""));
    assertThat(result, containsString("\"term\":{\"projects.name.keyword\":{\"value\":\"world\""));
    assertThat(result, containsString("\"path\":\"projects\""));
  }

  @Test
  public void testNestedSingleGroupBy() throws IOException {
    String result =
        explainQuery(
            "SELECT e.id, p.name "
                + "FROM opensearch-sql_test_index_employee_nested e, e.projects p "
                + "GROUP BY  p.name ");
    assertThat(result, containsString("\"terms\":{\"field\":\"projects.name.keyword\""));
    assertThat(result, containsString("\"nested\":{\"path\":\"projects\""));
  }

  @Test
  public void testNestedSingleOrderBy() throws IOException {
    String result =
        explainQuery(
            "SELECT e.id, p.name "
                + "FROM opensearch-sql_test_index_employee_nested e, e.projects p "
                + "ORDER BY p.name ");
    assertThat(result, containsString("\"sort\":[{\"projects.name.keyword\""));
    assertThat(result, containsString("\"nested\":{\"path\":\"projects\""));
  }

  @Test
  public void testNestedIsNotNullExplain() throws IOException {
    String explain =
        explainQuery(
            "SELECT e.name "
                + "FROM opensearch-sql_test_index_employee_nested as e, e.projects as p "
                + "WHERE p IS NOT NULL");

    assertThat(explain, containsString("\"exists\":{\"field\":\"projects\""));
    assertThat(explain, containsString("\"path\":\"projects\""));
  }

  @Test
  @Ignore // TODO: enable when subqueries are fixed
  public void testMultiQuery() throws IOException {
    String expectedOutput =
        TestUtils.fileToString("src/test/resources/expectedOutput/term_union_where", true);
    String result =
        explainQuery(
            "SELECT firstname "
                + "FROM opensearch-sql_test_index_account/account "
                + "WHERE firstname = 'Amber' "
                + "UNION ALL "
                + "SELECT dog_name as firstname "
                + "FROM opensearch-sql_test_index_dog/dog "
                + "WHERE holdersName = 'Hattie' OR dog_name = 'rex'");
    assertThat(result.replaceAll("\\s+", ""), equalTo(expectedOutput.replaceAll("\\s+", "")));
  }
}
