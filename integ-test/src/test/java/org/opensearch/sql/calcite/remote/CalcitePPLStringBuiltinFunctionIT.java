/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY_WITH_NULL;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.util.regex.PatternSyntaxException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLStringBuiltinFunctionIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testAscii() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/10?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"EeD\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where ascii(name) = 69 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("EeD", 27));
  }

  @Test
  public void testConcat() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name=concat('He', 'll', 'o') | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testConcatWithField() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"HelloWay\",\"age\":27,\"state\":\"Way\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name=concat('Hello', state) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("HelloWay", 27));
  }

  @Test
  public void testConcatWs() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"John,Way\",\"age\":27,\"state\":\"Way\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where name=concat_ws(',', 'John', state) | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("John,Way", 27));
  }

  @Test
  public void testLength() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where length(name) = 5 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLengthShouldBeInsensitive() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where leNgTh(name) = 5 | fields name, age", TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLower() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where lower(name) = 'hello' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testUpper() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where upper(name) = upper('hello') | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLike() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where like(name, '_ello%%') | fields name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testLocate() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where locate('Ja', name)=1 | eval locate3 = LOCATE('lll', 'llllll', 2)"
                    + " | fields name, age, locate3",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"), schema("locate3", "int"));

    verifyDataRows(actual, rows("Jake", 70, 2), rows("Jane", 20, 2));
  }

  @Test
  public void testSubstring() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where substring(name, 3, 2) = 'hn' | fields name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("John", 25));
  }

  @Test
  public void testSubstr() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where substr(name, 3, 2) = 'hn' | fields name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("John", 25));
  }

  @Test
  public void testPosition() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where position('ohn' in name) = 2 | fields name, age",
                TEST_INDEX_STATE_COUNTRY_WITH_NULL));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("John", 25));
  }

  @Test
  public void testTrim() throws IOException {
    prepareTrim();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where Trim(name) = 'Jim' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("   Jim", 27), rows("Jim   ", 57), rows("   Jim   ", 70));
  }

  @Test
  public void testRTrim() throws IOException {
    prepareTrim();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where RTrim(name) = 'Jim' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Jim   ", 57));
  }

  @Test
  public void testLTrim() throws IOException {
    prepareTrim();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where LTrim(name) = 'Jim' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("   Jim", 27));
  }

  @Test
  public void testReverse() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"DeD\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where Reverse(name) = name | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("DeD", 27));
  }

  @Test
  public void testRight() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"DeD\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where right(name, 2) = 'lo' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Hello", 30));
  }

  @Test
  public void testReplace() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where replace(name, 'J', 'M')='Mane' | eval hello = replace('hello',"
                    + " 'l', 'L') | fields name, age, hello",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"), schema("hello", "string"));

    verifyDataRows(actual, rows("Jane", 20, "heLLo"));
  }

  @Test
  public void testReplaceWithRegexPattern() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where account_number = 1 | eval street_only = replace(address,"
                    + " '\\\\d+ ', '') | fields address, street_only",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("address", "string"), schema("street_only", "string"));

    verifyDataRows(actual, rows("880 Holmes Lane", "Holmes Lane"));
  }

  @Test
  public void testReplaceWithCaptureGroups() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where account_number = 1 | eval swapped = replace(firstname,"
                    + " '^(.)(.)', '\\\\2\\\\1') | fields firstname, swapped",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("firstname", "string"), schema("swapped", "string"));

    verifyDataRows(actual, rows("Amber", "mAber"));
  }

  @Test
  public void testReplaceWithEmailDomainReplacement() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where account_number = 1 | eval new_email ="
                    + " replace(email, '([^@]+)@(.+)', '\\\\1@newdomain.com') | fields email,"
                    + " new_email",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("email", "string"), schema("new_email", "string"));

    verifyDataRows(actual, rows("amberduke@pyrami.com", "amberduke@newdomain.com"));
  }

  @Test
  public void testReplaceWithCharacterClasses() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where account_number = 1 | eval masked = replace(address, '[a-zA-Z]',"
                    + " 'X') | fields address, masked",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("address", "string"), schema("masked", "string"));

    verifyDataRows(actual, rows("880 Holmes Lane", "880 XXXXXX XXXX"));
  }

  @Test
  public void testReplaceWithAnchors() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where account_number = 1 | eval street_name = replace(address,"
                    + " '^\\\\d+\\\\s+', '') | fields address, street_name",
                TEST_INDEX_ACCOUNT));

    verifySchema(actual, schema("address", "string"), schema("street_name", "string"));

    verifyDataRows(actual, rows("880 Holmes Lane", "Holmes Lane"));
  }

  @Test
  public void testLeft() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where left(name, 2) = 'Ja' | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Jane", 20), rows("Jake", 70));
  }

  @Test
  public void testStrCmp() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where strcmp(name, 'Jane') = 0 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "int"));

    verifyDataRows(actual, rows("Jane", 20));
  }

  @Test
  public void testReplaceWithInvalidRegexPattern() {
    // Test invalid regex pattern - unclosed character class
    Throwable e1 =
        assertThrowsWithReplace(
            PatternSyntaxException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval result = replace(firstname, '[unclosed', 'X') | fields"
                            + " firstname, result",
                        TEST_INDEX_ACCOUNT)));
    verifyErrorMessageContains(e1, "Unclosed character class");
    verifyErrorMessageContains(e1, "400 Bad Request");

    // Test invalid regex pattern - unclosed group
    Throwable e2 =
        assertThrowsWithReplace(
            PatternSyntaxException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval result = replace(firstname, '(invalid', 'X') | fields"
                            + " firstname, result",
                        TEST_INDEX_ACCOUNT)));
    verifyErrorMessageContains(e2, "Unclosed group");
    verifyErrorMessageContains(e2, "400 Bad Request");

    // Test invalid regex pattern - dangling metacharacter
    Throwable e3 =
        assertThrowsWithReplace(
            PatternSyntaxException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | eval result = replace(firstname, '?invalid', 'X') | fields"
                            + " firstname, result",
                        TEST_INDEX_ACCOUNT)));
    verifyErrorMessageContains(e3, "Dangling meta character");
    verifyErrorMessageContains(e3, "400 Bad Request");
  }

  private void prepareTrim() throws IOException {
    Request request1 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/5?refresh=true");
    request1.setJsonEntity(
        "{\"name\":\"  "
            + " Jim\",\"age\":27,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request1);
    Request request2 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/6?refresh=true");
    request2.setJsonEntity(
        "{\"name\":\"Jim  "
            + " \",\"age\":57,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request2);
    Request request3 =
        new Request("PUT", "/opensearch-sql_test_index_state_country/_doc/7?refresh=true");
    request3.setJsonEntity(
        "{\"name\":\"   Jim  "
            + " \",\"age\":70,\"state\":\"B.C\",\"country\":\"Canada\",\"year\":2023,\"month\":4}");
    client().performRequest(request3);
  }
}
