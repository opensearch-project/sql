/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.sql;

import static org.hamcrest.Matchers.is;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.featureValueOf;

import java.io.IOException;
import java.util.Locale;
import java.util.function.Function;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.RestIntegTestCase;

/**
 * Integration test for different type of expressions such as literals, arithmetic, predicate
 * and function expression. Since comparison test in {@link SQLCorrectnessIT} is enforced,
 * this kind of manual written IT class will be focused on anomaly case test.
 */
@Ignore
public class ExpressionIT extends RestIntegTestCase {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Override
  protected void init() throws Exception {
    super.init();
  }

  public ResponseExceptionAssertion expectResponseException() {
    return new ResponseExceptionAssertion(exceptionRule);
  }

  /**
   * Response exception assertion helper to assert property value in OpenSearch ResponseException
   * and Response inside. This serves as syntax sugar to improve the readability of test
   * code.
   */
  private static class ResponseExceptionAssertion {
    private final ExpectedException exceptionRule;

    private ResponseExceptionAssertion(ExpectedException exceptionRule) {
      this.exceptionRule = exceptionRule;

      exceptionRule.expect(ResponseException.class);
    }

    ResponseExceptionAssertion hasStatusCode(int expected) {
      exceptionRule.expect(featureValueOf("statusCode", is(expected),
          (Function<ResponseException, Integer>) e ->
              e.getResponse().getStatusLine().getStatusCode()));
      return this;
    }

    ResponseExceptionAssertion containsMessage(String expected) {
      exceptionRule.expectMessage(expected);
      return this;
    }

    void whenExecute(String query) throws Exception {
      executeQuery(query);
    }
  }

  private static Response executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    return client().performRequest(request);
  }

}
