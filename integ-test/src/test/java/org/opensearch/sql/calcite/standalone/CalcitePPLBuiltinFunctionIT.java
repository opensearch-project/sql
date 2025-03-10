/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.closeTo;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.hamcrest.CoreMatchers.is;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
  }

  private static boolean containsMessage(Throwable throwable, String message) {
    while (throwable != null) {
      if (throwable.getMessage() != null && throwable.getMessage().contains(message)) {
        return true;
      }
      throwable = throwable.getCause();
    }
    return false;
  }

  @Test
  public void testSqrtAndCbrtAndPow() {
    JSONObject actual =
        executeQuery(
                String.format(
                        "source=%s | where sqrt(pow(age, 2)) = 30.0 and cbrt(pow(month, 3)) = 4 | fields name, age, month",
                        TEST_INDEX_STATE_COUNTRY));

    verifySchema(
            actual,
            schema("name", "string"),
            schema("age", "integer"),
            schema("month", "integer"));
    verifyDataRows(actual, rows("Hello", 30, 4));
  }

  @Test
  public void testSqrtNegativeArgShouldReturnNull() {
    JSONObject  actual = executeQuery(
            String.format(
                    "source=%s | head 1 | eval neg = sqrt(-1) | fields neg", TEST_INDEX_STATE_COUNTRY));
    verifyDataRows(actual, rows((Object) null));
  }

  @Test
  public void testSqrtNanArgShouldThrowError() {
    Exception nanException = assertThrows(
            IllegalStateException.class,
            () -> executeQuery(String.format(
                    "source=%s | head 1 | eval neg = sqrt('1') | fields neg", TEST_INDEX_STATE_COUNTRY)));
    assertTrue(containsMessage(nanException, "Invalid argument type: Expected a numeric value"));
  }

  @Test
  public void testSinAndCosAndAsinAndAcos() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | eval res = acos(cos(asin(sin(1)))) | head 1 | fields res",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("res", "double"));
    verifyDataRows(actual, closeTo(1.0));
  }

  @Test
  public void testAsinAndAcosInvalidArgShouldReturnNull() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | head 1 | eval s = asin(10), c = acos(-2) | fields s, c",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(
            actual,
            schema("s", "double"),
            schema("c", "double"));
    verifyDataRows(actual, rows(null, null));
  }

  @Test
  public void testAtanAndAtan2WithSort() {
      JSONObject actual =
              executeQuery(
                      String.format(
                              "source=%s | where month = atan(0) + 4 and age >= 30 + atan2(0, 1) | sort age | fields name, age, month",
                              TEST_INDEX_STATE_COUNTRY));

      verifySchema(actual, schema("name", "string"), schema("age", "integer"),
              schema("month", "integer"));
    verifyDataRowsInOrder(actual, rows("Hello", 30, 4), rows("Jake", 70, 4));
  }

  @Test
  public void testCeilingAndFloor() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | where age = ceiling(29.7) and month = floor(4.9) | fields name, age",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testConv() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | where age = conv('1E', 16, 10) | fields name, age",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testConvNegateValue() {
    JSONObject actual = executeQuery(
            String.format(
                    "source=%s | head 1 | eval hello = conv('29234652', 10, 36), negate = conv(-29234652, 10, 36) | fields hello, negate",
                    TEST_INDEX_STATE_COUNTRY
            ));
    verifyDataRows(actual, rows("hello", "-hello"));
  }

  @Test
  public void testConvWithInvalidRadix() {
    IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> executeQuery(
                    String.format(
                            "source=%s | eval invalid = conv('0000', 1, 36) | fields invalid", TEST_INDEX_STATE_COUNTRY)));
    assertThat(e.getCause().getCause().getMessage(), is("radix 1 less than Character.MIN_RADIX"));
  }

  @Test
  public void testPiAndCot() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | eval cot = cot(pi() / 4) | head 1 | fields cot",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("cot", "double"));
    verifyDataRows(actual, closeTo(1.0));
  }

  @Test
  public void testCrc32AndAbs() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | eval crc_name = crc32('Jane') | where crc32(name) = abs(0 - crc_name) | fields crc_name, name",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual,
            schema("crc_name", "long"),
            schema("name", "string"));
    verifyDataRows(actual, rows(1516115372L, "Jane"));
  }

  @Test
  public void testEAndLn() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | eval ln_e = ln(e()) | head 1 | fields ln_e",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("ln_e", "double"));
    verifyDataRows(actual, closeTo(1.0));
  }

  @Test
  public void testExpAndFloor() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | where age = floor(exp(3.41)) | fields name, age",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(
            actual,
            schema("name", "string"),
            schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLogAndLog2AndLog10() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | head 1 | eval log =  log(30, 900), log2 = log2(4), log10 = log10(1000)  | " +
                                    "fields log, log2, log10",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual,
            schema("log", "double"),
            schema("log2", "double"),
            schema("log10", "double"));
    verifyDataRows(actual, closeTo(2, 2, 3));
  }

  @Test
  public void testModWithSortAndFields() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | where mod(age, 10) = 0 | sort -age | fields name, age",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRowsInOrder(
            actual,
            rows("Jake", 70),
            rows("Hello", 30),
            rows("Jane", 20)
            );
  }

  @Test
  public void testModFloatAndNegative() {
    JSONObject actual = executeQuery(
            String.format(
                    "source=%s | head 1 | eval f = mod(3.1, 2), n = mod(-3, 2) | fields f, n",
                    TEST_INDEX_STATE_COUNTRY));
    verifySchema(actual,
            schema("f", "double"),
            schema("n", "integer"));
    verifyDataRows(actual, closeTo(1.1, 1));
  }

  @Test
  public void testModShouldReturnWiderType() {
    JSONObject actual = executeQuery(
            String.format(
                    "source=%s | head 1 | eval i = mod(2147483647, 2), l = mod(2147483648, 2), " +
                            "d = mod(3, 2.1) | fields i, l, d",
                    TEST_INDEX_STATE_COUNTRY));
    verifySchema(
            actual, schema("i", "integer"),
            schema("l", "long"),
            schema("d", "double"));
    verifyDataRows(actual, closeTo(1, 0, 0.9));
  }

  @Test
  public void testModByZeroShouldReturnNull() {
    JSONObject actual = executeQuery(
            String.format(
                    "source=%s | head 1 | eval z = mod(5, 0) | fields z",
                    TEST_INDEX_STATE_COUNTRY
    ));
    verifySchema(actual, schema("z", "integer"));
    verifyDataRows(actual, rows((Object) null));
  }

  @Test
  public void testRadiansAndDegrees() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | head 1 | eval r = radians(degrees(0.5)) | fields r",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("r", "double"));
    verifyDataRows(actual, closeTo(0.5));
  }

  @Test
  public void testRand() {
      JSONObject actual =
              executeQuery(
                      String.format(
                              "source=%s | eval rand = rand() | where rand > 0 | where rand < 1  | fields name",
                              TEST_INDEX_STATE_COUNTRY));

      verifySchema(actual, schema("name", "string"));
      verifyDataRows(actual, rows("Jake"), rows("Hello"), rows("Jane"), rows("John"));
  }

  @Test
  public void testPowInvalidArgShouldReturnNull() {
    JSONObject actual =
            executeQuery(
                    String.format(
                            "source=%s | head 1 | eval res = pow(-3, 0.5)  | fields res",
                            TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("res", "double"));
    verifyDataRows(actual, rows((Object) null));
  }

    @Test
    public void testSignAndRound() {
        JSONObject actual =
                executeQuery(
                        String.format(
                                "source=%s | eval thirty_one = round(30.9) |  where age = sign(-3) + thirty_one | " +
                                        "fields name, age, thirty_one",
                                TEST_INDEX_STATE_COUNTRY));

        verifySchema(
                actual,
                schema("name", "string"),
                schema("age", "integer"),
                schema("thirty_one", "double"));
        verifyDataRows(actual, rows("Hello", 30, 31));
    }
}
