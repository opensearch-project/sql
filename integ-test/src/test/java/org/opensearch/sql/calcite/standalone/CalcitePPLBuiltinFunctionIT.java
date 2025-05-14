/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NULL_MISSING;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.util.MatcherUtils.closeTo;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.DATA_TYPE_NUMERIC);
    loadIndex(Index.DOG);
    loadIndex(Index.NULL_MISSING);
  }

  @Test
  public void testSqrtAndCbrtAndPow() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where sqrt(pow(age, 2)) = 30.0 and cbrt(pow(month, 3)) = 4 | fields"
                    + " name, age, month",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        actual, schema("name", "string"), schema("age", "integer"), schema("month", "integer"));
    verifyDataRows(actual, rows("Hello", 30, 4));
  }

  @Test
  public void testSqrtNegativeArgShouldReturnNull() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval neg = sqrt(-1 * age) | fields neg",
                TEST_INDEX_STATE_COUNTRY));
    verifyDataRows(actual, rows((Object) null));
  }

  // TODO: Enable it once parameter validation for UDF is ready
  @Ignore
  @Test
  public void testSqrtNanArgShouldThrowError() {
    Exception nanException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | head 1 | eval sqrt_name = sqrt(name) | fields sqrt_name",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(nanException, "Invalid argument type: Expected a numeric value");
  }

  @Test
  public void testSinAndCosAndAsinAndAcos() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where month = 4 | head 1 | eval res = acos(cos(asin(sin(month / 4))))"
                    + " | head 1 | fields res",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("res", "double"));
    verifyDataRows(actual, closeTo(1.0));
  }

  @Test
  public void testAsinAndAcosInvalidArgShouldReturnNull() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where byte_number > 1 | head 1 | eval s = asin(byte_number), c ="
                    + " acos(-1 * byte_number) | fields s, c",
                TEST_INDEX_DATATYPE_NUMERIC));

    verifySchema(actual, schema("s", "double"), schema("c", "double"));
    verifyDataRows(actual, rows(null, null));
  }

  @Test
  public void testAtanAndAtan2WithSort() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where month = atan(0) + 4 and age >= 30 + atan2(0, 1) | sort age |"
                    + " fields name, age, month",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        actual, schema("name", "string"), schema("age", "integer"), schema("month", "integer"));
    verifyDataRowsInOrder(actual, rows("Hello", 30, 4), rows("Jake", 70, 4));
  }

  @Test
  public void testTypeOfBasic() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                    source=%s
                    | eval `typeof(1)` = typeof(1)
                    | eval `typeof(true)` = typeof(true)
                    | eval `typeof(2.0)` = typeof(2.0)
                    | eval `typeof("2.0")` = typeof("2.0")
                    | eval `typeof(name)` = typeof(name)
                    | eval `typeof(country)` = typeof(country)
                    | eval `typeof(age)` = typeof(age)
                    | eval `typeof(interval)` = typeof(INTERVAL 2 DAY)
                    | fields `typeof(1)`, `typeof(true)`, `typeof(2.0)`, `typeof("2.0")`, `typeof(name)`, `typeof(country)`, `typeof(age)`, `typeof(interval)`
                    | head 1
                    """,
                TEST_INDEX_STATE_COUNTRY));
    verifyDataRows(
        result, rows("INT", "BOOLEAN", "DOUBLE", "STRING", "STRING", "STRING", "INT", "INTERVAL"));
  }

  public void testTypeOfDateTime() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                    source=%s
                    | eval `typeof(date)` = typeof(DATE('2008-04-14'))
                    | eval `typeof(now())` = typeof(now())
                    | fields `typeof(date)`, `typeof(now())`
                    """,
                TEST_INDEX_STATE_COUNTRY));
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
  public void testConvAndLower() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where lower(name) = conv('29234652', 10, 36) | fields name",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("Hello"));
  }

  @Test
  public void testConvNegateValue() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where dog_name = conv('1732835878', 10, 36) | eval negate ="
                    + " conv('-1732835878', 10, 36), number = conv(dog_name, 36, 10) | fields"
                    + " dog_name, negate, number",
                TEST_INDEX_DOG));
    verifySchema(
        actual,
        schema("dog_name", "string"),
        schema("negate", "string"),
        schema("number", "string"));
    verifyDataRows(actual, rows("snoopy", "-snoopy", "1732835878"));
  }

  @Test
  public void testConvWithInvalidRadix() {
    Exception invalidRadixException =
        assertThrows(
            NumberFormatException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval invalid = conv('0000', 1, 36) | fields invalid",
                        TEST_INDEX_STATE_COUNTRY)));
    verifyErrorMessageContains(invalidRadixException, "radix 1 less than Character.MIN_RADIX");
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
                "source=%s | eval crc_name = crc32('Jane') | where crc32(name) = abs(0 - crc_name)"
                    + " | fields crc_name, name",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("crc_name", "long"), schema("name", "string"));
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

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLogAndLog2AndLog10() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval log =  log(30, 900), log2 = log2(4), log10 = log10(1000)"
                    + "  | fields log, log2, log10",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        actual, schema("log", "double"), schema("log2", "double"), schema("log10", "double"));
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

    verifyDataRowsInOrder(actual, rows("Jake", 70), rows("Hello", 30), rows("Jane", 20));
  }

  @Test
  public void testModFloatAndNegative() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval f = mod(float_number, 2), n = -1 * short_number %% 2, nd = -1 *"
                    + " double_number %% 2 | fields f, n, nd",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(actual, schema("f", "float"), schema("n", "integer"), schema("nd", "double"));
    verifyDataRows(actual, closeTo(0.2, -1, -1.1));
  }

  @Test
  public void testModShouldReturnWiderTypes() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval b = byte_number %% 2, i = mod(integer_number, 3), l ="
                    + " mod(long_number, 2), f = float_number %% 2, d = mod(double_number, 2), s ="
                    + " short_number %% byte_number | fields b, i, l, f, d, s",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(
        actual,
        schema("b", "integer"),
        schema("i", "integer"),
        schema("l", "long"),
        schema("f", "float"),
        schema("d", "double"),
        schema("s", "short"));
    verifyDataRows(actual, closeTo(0, 2, 1, 0.2, 1.1, 3));
  }

  @Test
  public void testModByZeroShouldReturnNull() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval z = mod(5, 0) | fields z", TEST_INDEX_STATE_COUNTRY));
    verifySchema(actual, schema("z", "integer"));
    verifyDataRows(actual, rows((Object) null));
  }

  // TODO: Enable it once parameter validation for UDF is ready
  @Ignore
  @Test
  public void testMod3ArgsShouldThrowIllegalArgError() {
    Exception wrongArgException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval z = mod(float_number, integer_number, byte_number) |"
                            + " fields z",
                        TEST_INDEX_DATATYPE_NUMERIC)));
    verifyErrorMessageContains(wrongArgException, "MOD function requires exactly two arguments");
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
                "source=%s | eval thirty_one = round(30.9) |  where age = sign(-3) + thirty_one | "
                    + "fields name, age, thirty_one",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        actual, schema("name", "string"), schema("age", "integer"), schema("thirty_one", "double"));
    verifyDataRows(actual, rows("Hello", 30, 31));
  }

  @Test
  public void testDivide() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval r1 = 22 / 7, r2 = integer_number / 1, r3 = 21 / 7, r4 ="
                    + " byte_number / short_number, r5 = half_float_number / float_number, r6 ="
                    + " float_number / short_number, r7 = 22 / 7.0, r8 = 22.0 / 7, r9 = 21.0 / 7.0,"
                    + " r10 = half_float_number / short_number, r11 = double_number / float_number"
                    + " | fields r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifySchema(
        actual,
        schema("r1", "integer"),
        schema("r2", "integer"),
        schema("r3", "integer"),
        schema("r4", "short"),
        schema("r5", "float"),
        schema("r6", "float"),
        schema("r7", "double"),
        schema("r8", "double"),
        schema("r9", "double"),
        schema("r10", "float"),
        schema("r11", "double"));
    verifyDataRows(
        actual,
        closeTo(
            3,
            2,
            3,
            1,
            1.1774194,
            2.0666666,
            3.142857142857143,
            3.142857142857143,
            3.0,
            2.4333334,
            0.8225806704669051));
  }

  @Test
  public void testDivideShouldReturnNull() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where key = 'null' | head 1 | eval r2 = 4 / dbl, r3 = `int` / 5, r4 ="
                    + " 22 / 0, r5 = 22.0 / 0, r6 = 22.0 / 0.0 | fields r2, r3, r4, r5, r6",
                TEST_INDEX_NULL_MISSING));
    verifySchema(
        actual,
        schema("r2", "double"),
        schema("r3", "integer"),
        schema("r4", "integer"),
        schema("r5", "double"),
        schema("r6", "double"));
    verifyDataRows(actual, rows(null, null, null, null, null));
  }
}
