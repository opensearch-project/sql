/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class TextFunctionIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
  }

  void verifyQuery(String query, String type, String output) throws IOException {
    JSONObject result = executeQuery("select " + query);
    verifySchema(result, schema(query, null, type));
    verifyDataRows(result, rows(output));
  }

  void verifyQuery(String query, String type, Integer output) throws IOException {
    JSONObject result = executeQuery("select " + query);
    verifySchema(result, schema(query, null, type));
    verifyDataRows(result, rows(output));
  }

  void verifyQueryWithNullOutput(String query, String type) throws IOException {
    JSONObject result = executeQuery("select 'test null'," + query);
    verifySchema(result, schema(query, null, type), schema("'test null'", null, type));
    verifyDataRows(result, rows("test null", null));
  }

  @Test
  public void testRegexp() throws IOException {
    verifyQuery("'a' regexp 'b'", "integer", 0);
    verifyQuery("'a' regexp '.*'", "integer", 1);
  }

  @Test
  public void testReverse() throws IOException {
    verifyQuery("reverse('hello')", "keyword", "olleh");
    verifyQuery("reverse('')", "keyword", "");
    verifyQueryWithNullOutput("reverse(null)", "keyword");
  }

  @Test
  public void testSubstr() throws IOException {
    verifyQuery("substr('hello', 2)", "keyword", "ello");
    verifyQuery("substr('hello', 2, 2)", "keyword", "el");
  }

  @Test
  public void testSubstring() throws IOException {
    verifyQuery("substring('hello', 2)", "keyword", "ello");
    verifyQuery("substring('hello', 2, 2)", "keyword", "el");
  }

  @Test
  public void testUpper() throws IOException {
    verifyQuery("upper('hello')", "keyword", "HELLO");
    verifyQuery("upper('HELLO')", "keyword", "HELLO");
  }

  @Test
  public void testLower() throws IOException {
    verifyQuery("lower('hello')", "keyword", "hello");
    verifyQuery("lower('HELLO')", "keyword", "hello");
  }

  @Test
  public void testTrim() throws IOException {
    verifyQuery("trim(' hello')", "keyword", "hello");
    verifyQuery("trim('hello ')", "keyword", "hello");
    verifyQuery("trim('  hello  ')", "keyword", "hello");
  }

  @Test
  public void testRtrim() throws IOException {
    verifyQuery("rtrim(' hello')", "keyword", " hello");
    verifyQuery("rtrim('hello ')", "keyword", "hello");
    verifyQuery("rtrim('  hello  ')", "keyword", "  hello");
  }

  @Test
  public void testLtrim() throws IOException {
    verifyQuery("ltrim(' hello')", "keyword", "hello");
    verifyQuery("ltrim('hello ')", "keyword", "hello ");
    verifyQuery("ltrim('  hello  ')", "keyword", "hello  ");
  }

  @Test
  public void testConcat() throws IOException {
    verifyQuery("concat('hello', 'whole', 'world', '!', '!')", "keyword", "hellowholeworld!!");
    verifyQuery("concat('hello', 'world')", "keyword", "helloworld");
    verifyQuery("concat('', 'hello')", "keyword", "hello");
  }

  @Test
  public void testConcat_ws() throws IOException {
    verifyQuery("concat_ws(',', 'hello', 'world')", "keyword", "hello,world");
    verifyQuery("concat_ws(',', '', 'hello')", "keyword", ",hello");
  }

  @Test
  public void testLength() throws IOException {
    verifyQuery("length('hello')", "integer", 5);
  }

  @Test
  public void testStrcmp() throws IOException {
    verifyQuery("strcmp('hello', 'world')", "integer", -1);
    verifyQuery("strcmp('hello', 'hello')", "integer", 0);
  }

  @Test
  public void testRight() throws IOException {
    verifyQuery("right('variable', 4)", "keyword", "able");
  }

  @Test
  public void testLeft() throws IOException {
    verifyQuery("left('variable', 3)", "keyword", "var");
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
