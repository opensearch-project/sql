/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
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

public class MathematicalFunctionIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testPI() throws IOException {
    JSONObject result =
            executeQuery(String.format("SELECT PI() FROM %s HAVING (COUNT(1) > 0)",TEST_INDEX_BANK) );
    verifySchema(result,
            schema("PI()", null, "double"));
    verifyDataRows(result, rows(3.141592653589793));
  }

  @Test
  public void testCeil() throws IOException {
    JSONObject result = executeQuery("select ceil(0)");
    verifySchema(result, schema("ceil(0)", null, "long"));
    verifyDataRows(result, rows(0));

    result = executeQuery("select ceil(2147483646.9)");
    verifySchema(result, schema("ceil(2147483646.9)", null, "long"));
    verifyDataRows(result, rows(2147483647));

    result = executeQuery("select ceil(92233720368547807.9)");
    verifySchema(result, schema("ceil(92233720368547807.9)", null, "long"));
    verifyDataRows(result, rows(92233720368547808L));
  }

  @Test
  public void testConv() throws IOException {
    JSONObject result = executeQuery("select conv(11, 10, 16)");
    verifySchema(result, schema("conv(11, 10, 16)", null, "keyword"));
    verifyDataRows(result, rows("b"));

    result = executeQuery("select conv(11, 16, 10)");
    verifySchema(result, schema("conv(11, 16, 10)", null, "keyword"));
    verifyDataRows(result, rows("17"));
  }

  @Test
  public void testCosh() throws IOException {
    JSONObject result = executeQuery("select cosh(1)");
    verifySchema(result, schema("cosh(1)", null, "double"));
    verifyDataRows(result, rows(1.543080634815244));

    result = executeQuery("select cosh(-1)");
    verifySchema(result, schema("cosh(-1)", null, "double"));
    verifyDataRows(result, rows(1.543080634815244));

    result = executeQuery("select cosh(1.5)");
    verifySchema(result, schema("cosh(1.5)", null, "double"));
    verifyDataRows(result, rows(2.352409615243247));
  }

  @Test
  public void testCrc32() throws IOException {
    JSONObject result = executeQuery("select crc32('MySQL')");
    verifySchema(result, schema("crc32('MySQL')", null, "long"));
    verifyDataRows(result, rows(3259397556L));
  }

  @Test
  public void testE() throws IOException {
    JSONObject result = executeQuery("select e()");
    verifySchema(result, schema("e()", null, "double"));
    verifyDataRows(result, rows(Math.E));
  }

  @Test
  public void testExpm1() throws IOException {
    JSONObject result = executeQuery("select expm1(account_number) FROM " + TEST_INDEX_BANK + " LIMIT 2");
    verifySchema(result, schema("expm1(account_number)", null, "double"));
    verifyDataRows(result, rows(Math.expm1(1)), rows(Math.expm1(6)));
  }

  @Test
  public void testMod() throws IOException {
    JSONObject result = executeQuery("select mod(3, 2)");
    verifySchema(result, schema("mod(3, 2)", null, "integer"));
    verifyDataRows(result, rows(1));

    result = executeQuery("select mod(3.1, 2)");
    verifySchema(result, schema("mod(3.1, 2)", null, "double"));
    verifyDataRows(result, rows(1.1));
  }

  @Test
  public void testPow() throws IOException {
    JSONObject result = executeQuery("select pow(3, 2)");
    verifySchema(result, schema("pow(3, 2)", null, "double"));
    verifyDataRows(result, rows(9.0));

    result = executeQuery("select pow(0, 2)");
    verifySchema(result, schema("pow(0, 2)", null, "double"));
    verifyDataRows(result, rows(0.0));

    result = executeQuery("select pow(3, 0)");
    verifySchema(result, schema("pow(3, 0)", null, "double"));
    verifyDataRows(result, rows(1.0));

    result = executeQuery("select pow(-2, 3)");
    verifySchema(result, schema("pow(-2, 3)", null, "double"));
    verifyDataRows(result, rows(-8.0));

    result = executeQuery("select pow(2, -2)");
    verifySchema(result, schema("pow(2, -2)", null, "double"));
    verifyDataRows(result, rows(0.25));

    result = executeQuery("select pow(-2, -3)");
    verifySchema(result, schema("pow(-2, -3)", null, "double"));
    verifyDataRows(result, rows(-0.125));

    result = executeQuery("select pow(-1, 0.5)");
    verifySchema(result, schema("pow(-1, 0.5)", null, "double"));
    verifyDataRows(result, rows((Object) null));
  }

  @Test
  public void testPower() throws IOException {
    JSONObject result = executeQuery("select power(3, 2)");
    verifySchema(result, schema("power(3, 2)", null, "double"));
    verifyDataRows(result, rows(9.0));

    result = executeQuery("select power(0, 2)");
    verifySchema(result, schema("power(0, 2)", null, "double"));
    verifyDataRows(result, rows(0.0));

    result = executeQuery("select power(3, 0)");
    verifySchema(result, schema("power(3, 0)", null, "double"));
    verifyDataRows(result, rows(1.0));

    result = executeQuery("select power(-2, 3)");
    verifySchema(result, schema("power(-2, 3)", null, "double"));
    verifyDataRows(result, rows(-8.0));

    result = executeQuery("select power(2, -2)");
    verifySchema(result, schema("power(2, -2)", null, "double"));
    verifyDataRows(result, rows(0.25));

    result = executeQuery("select power(2, -2)");
    verifySchema(result, schema("power(2, -2)", null, "double"));
    verifyDataRows(result, rows(0.25));

    result = executeQuery("select power(-2, -3)");
    verifySchema(result, schema("power(-2, -3)", null, "double"));
    verifyDataRows(result, rows(-0.125));
  }

  @Test
  public void testRint() throws IOException {
    JSONObject result = executeQuery("select rint(56.78)");
    verifySchema(result, schema("rint(56.78)", null, "double"));
    verifyDataRows(result, rows(57.0));

    result = executeQuery("select rint(-56)");
    verifySchema(result, schema("rint(-56)", null, "double"));
    verifyDataRows(result, rows(-56.0));

    result = executeQuery("select rint(3.5)");
    verifySchema(result, schema("rint(3.5)", null, "double"));
    verifyDataRows(result, rows(4.0));

    result = executeQuery("select rint(-3.5)");
    verifySchema(result, schema("rint(-3.5)", null, "double"));
    verifyDataRows(result, rows(-4.0));
  }

  @Test
  public void testRound() throws IOException {
    JSONObject result = executeQuery("select round(56.78)");
    verifySchema(result, schema("round(56.78)", null, "double"));
    verifyDataRows(result, rows(57.0));

    result = executeQuery("select round(56.78, 1)");
    verifySchema(result, schema("round(56.78, 1)", null, "double"));
    verifyDataRows(result, rows(56.8));

    result = executeQuery("select round(56.78, -1)");
    verifySchema(result, schema("round(56.78, -1)", null, "double"));
    verifyDataRows(result, rows(60.0));

    result = executeQuery("select round(-56)");
    verifySchema(result, schema("round(-56)", null, "long"));
    verifyDataRows(result, rows(-56));

    result = executeQuery("select round(-56, 1)");
    verifySchema(result, schema("round(-56, 1)", null, "long"));
    verifyDataRows(result, rows(-56));

    result = executeQuery("select round(-56, -1)");
    verifySchema(result, schema("round(-56, -1)", null, "long"));
    verifyDataRows(result, rows(-60));

    result = executeQuery("select round(3.5)");
    verifySchema(result, schema("round(3.5)", null, "double"));
    verifyDataRows(result, rows(4.0));

    result = executeQuery("select round(-3.5)");
    verifySchema(result, schema("round(-3.5)", null, "double"));
    verifyDataRows(result, rows(-4.0));
  }

  @Test
  public void testSign() throws IOException {
    JSONObject result = executeQuery("select sign(1.1)");
    verifySchema(result, schema("sign(1.1)", null, "integer"));
    verifyDataRows(result, rows(1));

    result = executeQuery("select sign(-1.1)");
    verifySchema(result, schema("sign(-1.1)", null, "integer"));
    verifyDataRows(result, rows(-1));
  }

  @Test
  public void testSignum() throws IOException {
    JSONObject result = executeQuery("select signum(1.1)");
    verifySchema(result, schema("signum(1.1)", null, "integer"));
    verifyDataRows(result, rows(1));

    result = executeQuery("select signum(-1.1)");
    verifySchema(result, schema("signum(-1.1)", null, "integer"));
    verifyDataRows(result, rows(-1));
  }

  public void testSinh() throws IOException {
    JSONObject result = executeQuery("select sinh(1)");
    verifySchema(result, schema("sinh(1)", null, "double"));
    verifyDataRows(result, rows(1.1752011936438014));

    result = executeQuery("select sinh(-1)");
    verifySchema(result, schema("sinh(-1)", null, "double"));
    verifyDataRows(result, rows(-1.1752011936438014));

    result = executeQuery("select sinh(1.5)");
    verifySchema(result, schema("sinh(1.5)", null, "double"));
    verifyDataRows(result, rows(2.1292794550948173));
  }

  @Test
  public void testTruncate() throws IOException {
    JSONObject result = executeQuery("select truncate(56.78, 1)");
    verifySchema(result, schema("truncate(56.78, 1)", null, "double"));
    verifyDataRows(result, rows(56.7));

    result = executeQuery("select truncate(56.78, -1)");
    verifySchema(result, schema("truncate(56.78, -1)", null, "double"));
    verifyDataRows(result, rows(50.0));

    result = executeQuery("select truncate(-56, 1)");
    verifySchema(result, schema("truncate(-56, 1)", null, "long"));
    verifyDataRows(result, rows(-56));

    result = executeQuery("select truncate(-56, -1)");
    verifySchema(result, schema("truncate(-56, -1)", null, "long"));
    verifyDataRows(result, rows(-50));

    result = executeQuery("select truncate(33.33344, -1)");
    verifySchema(result, schema("truncate(33.33344, -1)", null, "double"));
    verifyDataRows(result, rows(30.0));

    result = executeQuery("select truncate(33.33344, 2)");
    verifySchema(result, schema("truncate(33.33344, 2)", null, "double"));
    verifyDataRows(result, rows(33.33));

    result = executeQuery("select truncate(33.33344, 100)");
    verifySchema(result, schema("truncate(33.33344, 100)", null, "double"));
    verifyDataRows(result, rows(33.33344));

    result = executeQuery("select truncate(33.33344, 0)");
    verifySchema(result, schema("truncate(33.33344, 0)", null, "double"));
    verifyDataRows(result, rows(33.0));

    result = executeQuery("select truncate(33.33344, 4)");
    verifySchema(result, schema("truncate(33.33344, 4)", null, "double"));
    verifyDataRows(result, rows(33.3334));

    result = executeQuery(String.format("select truncate(%s, 6)", Math.PI));
    verifySchema(result, schema(String.format("truncate(%s, 6)", Math.PI), null, "double"));
    verifyDataRows(result, rows(3.141592));
  }

  @Test
  public void testAtan() throws IOException {
    JSONObject result = executeQuery("select atan(2, 3)");
    verifySchema(result, schema("atan(2, 3)", null, "double"));
    verifyDataRows(result, rows(Math.atan2(2, 3)));
  }

  @Test
  public void testCbrt() throws IOException {
    JSONObject result = executeQuery("select cbrt(8)");
    verifySchema(result, schema("cbrt(8)", "double"));
    verifyDataRows(result, rows(2.0));

    result = executeQuery("select cbrt(9.261)");
    verifySchema(result, schema("cbrt(9.261)", "double"));
    verifyDataRows(result, rows(2.1));

    result = executeQuery("select cbrt(-27)");
    verifySchema(result, schema("cbrt(-27)", "double"));
    verifyDataRows(result, rows(-3.0));
  }

  @Test
  public void testLnReturnsNull() throws IOException {
    JSONObject result = executeQuery("select ln(0), ln(-2)");
    verifySchema(result,
        schema("ln(0)", "double"),
        schema("ln(-2)", "double"));
    verifyDataRows(result, rows(null, null));
  }

  @Test
  public void testLogReturnsNull() throws IOException {
    JSONObject result = executeQuery("select log(0), log(-2)");
    verifySchema(result,
        schema("log(0)", "double"),
        schema("log(-2)", "double"));
    verifyDataRows(result, rows(null, null));
  }

  @Test
  public void testLog10ReturnsNull() throws IOException {
    JSONObject result = executeQuery("select log10(0), log10(-2)");
    verifySchema(result,
        schema("log10(0)", "double"),
        schema("log10(-2)", "double"));
    verifyDataRows(result, rows(null, null));
  }

  @Test
  public void testLog2ReturnsNull() throws IOException {
    JSONObject result = executeQuery("select log2(0), log2(-2)");
    verifySchema(result,
        schema("log2(0)", "double"),
        schema("log2(-2)", "double"));
    verifyDataRows(result, rows(null, null));
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
