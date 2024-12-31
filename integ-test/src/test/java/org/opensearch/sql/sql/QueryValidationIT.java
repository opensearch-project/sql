/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.is;
import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.featureValueOf;

import java.io.IOException;
import java.util.Locale;
import java.util.function.Function;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * The query validation IT only covers test for error cases that not doable in comparison test. For
 * all other tests, comparison test should be favored over manual written test like this.
 */
public class QueryValidationIT extends SQLIntegTestCase {

  @Rule public final ExpectedException exceptionRule = ExpectedException.none();

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Ignore(
      "Will add this validation in analyzer later. This test should be enabled once "
          + "https://github.com/opensearch-project/sql/issues/910 has been resolved")
  @Test
  public void testNonAggregatedSelectColumnMissingInGroupByClause() throws IOException {
    expectResponseException()
        .hasStatusCode(BAD_REQUEST)
        .hasErrorType("SemanticCheckException")
        .containsMessage(
            "Expression [state] that contains non-aggregated column "
                + "is not present in group by clause")
        .whenExecute("SELECT state FROM opensearch-sql_test_index_account GROUP BY age");
  }

  @Test
  public void testNonAggregatedSelectColumnPresentWithoutGroupByClause() throws IOException {
    expectResponseException()
        .hasStatusCode(BAD_REQUEST)
        .hasErrorType("SemanticCheckException")
        .containsMessage(
            "Explicit GROUP BY clause is required because expression [state] "
                + "contains non-aggregated column")
        .whenExecute("SELECT state, AVG(age) FROM opensearch-sql_test_index_account");
  }

  @Test
  public void testQueryFieldWithKeyword() throws IOException {
    expectResponseException()
        .hasStatusCode(BAD_REQUEST)
        .hasErrorType("SemanticCheckException")
        .containsMessage(
            "can't resolve Symbol(namespace=FIELD_NAME, name=firstname.keyword) in type env")
        .whenExecute("SELECT firstname.keyword FROM opensearch-sql_test_index_account");
  }

  @Test
  public void aggregationFunctionInSelectGroupByMultipleFields() throws IOException {
    expectResponseException()
        .hasStatusCode(BAD_REQUEST)
        .hasErrorType("SemanticCheckException")
        .containsMessage(
            "can't resolve Symbol(namespace=FIELD_NAME, name=state.keyword) in type env")
        .whenExecute(
            "SELECT SUM(age) FROM opensearch-sql_test_index_account GROUP BY state.keyword");
  }

  public ResponseExceptionAssertion expectResponseException() {
    return new ResponseExceptionAssertion(exceptionRule);
  }

  /**
   * Response exception assertion helper to assert property value in OpenSearch ResponseException
   * and Response inside. This serves as syntax sugar to improve the readability of test code.
   */
  private static class ResponseExceptionAssertion {
    private final ExpectedException exceptionRule;

    private ResponseExceptionAssertion(ExpectedException exceptionRule) {
      this.exceptionRule = exceptionRule;

      exceptionRule.expect(ResponseException.class);
    }

    ResponseExceptionAssertion hasStatusCode(RestStatus code) {
      exceptionRule.expect(
          featureValueOf(
              "statusCode",
              is(code),
              (Function<ResponseException, RestStatus>)
                  e -> RestStatus.fromCode(e.getResponse().getStatusLine().getStatusCode())));
      return this;
    }

    ResponseExceptionAssertion hasErrorType(String type) {
      exceptionRule.expectMessage("\"type\": \"" + type + "\"");
      return this;
    }

    ResponseExceptionAssertion containsMessage(String... messages) {
      for (String message : messages) {
        exceptionRule.expectMessage(message);
      }
      return this;
    }

    void whenExecute(String query) throws IOException {
      execute(query);
    }
  }

  private static void execute(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    client().performRequest(request);
  }
}
