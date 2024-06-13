/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TEXT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TEXT_FOR_CAST;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import java.util.Locale;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class TextTypeIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    loadIndex(Index.TEXT);
    loadIndex(Index.TEXT_FOR_CAST);
  }

  @Test
  public void test_all_data_returned_as_strings() {
    JSONObject result = executeQuery("select * FROM " + TEST_INDEX_TEXT);
    verifySchema(
        result,
        schema("TextWithKeywords", null, "text"),
        schema("TextWithFielddata", null, "text"),
        schema("Keyword", null, "keyword"),
        schema("Text", null, "text"),
        schema("TextWithFields", null, "text"),
        schema("TextWithMixedFields", null, "text"),
        schema("TextWithNumbers", null, "text"));
    verifyDataRows(
        result,
        rows("Text With Keywords", "Text", "K for Keyword", null, null, "one two tree", null),
        rows(null, "Fielddata is the best!", null, "Text", "Fields for freedom", "42", "42"),
        rows("Another Text With Similar Keywords", null, "ikiki", null, null, "12.04.1961", "42"),
        rows("", "Text With Fielddata", null, "Some text", "Freedom for fields", null, "100500"),
        rows(
            null,
            null,
            "kokoko",
            "A fairy tale about little mermaid",
            "A Text with a Field",
            null,
            null));
  }

  @Test
  public void test_ascii() {
    JSONObject result =
        executeQuery(
            "select ascii(Keyword), ascii(TextWithKeywords), ascii(Text),ascii(TextWithFielddata),"
                + "ascii(TextWithFields), ascii(TextWithNumbers), ascii(TextWithMixedFields) FROM "
                + TEST_INDEX_TEXT);

    verifySchema(
        result,
        schema("ascii(Keyword)", null, "integer"),
        schema("ascii(TextWithKeywords)", null, "integer"),
        schema("ascii(Text)", null, "integer"),
        schema("ascii(TextWithFielddata)", null, "integer"),
        schema("ascii(TextWithFields)", null, "integer"),
        schema("ascii(TextWithNumbers)", null, "integer"),
        schema("ascii(TextWithMixedFields)", null, "integer"));
    verifyDataRows(
        result,
        rows(75, 84, null, 84, null, null, 111),
        rows(null, null, 84, 70, 70, 52, 52),
        rows(105, 65, null, null, null, 52, 49),
        rows(null, 0, 83, 84, 70, 49, null),
        rows(107, null, 65, null, 65, null, null));
  }

  @Test
  public void test_concat_with_user_string() {
    JSONObject result =
        executeQuery(
            "select concat(Keyword), concat(Keyword, 'ab'), "
                + "concat(TextWithKeywords), concat(TextWithKeywords, 'bc'), "
                + "concat(Text), concat(Text, 'bc'), "
                + "concat(TextWithFielddata), concat(TextWithFielddata, 'cd'), "
                + "concat(TextWithFields), concat(TextWithFields, 'de'), "
                + "concat(TextWithNumbers), concat(TextWithNumbers, 'ef'), "
                + "concat(TextWithMixedFields), concat(TextWithMixedFields, 'fg') FROM "
                + TEST_INDEX_TEXT);

    verifySchema(
        result,
        schema("concat(Keyword)", null, "keyword"),
        schema("concat(Keyword, 'ab')", null, "keyword"),
        schema("concat(TextWithKeywords)", null, "keyword"),
        schema("concat(TextWithKeywords, 'bc')", null, "keyword"),
        schema("concat(Text)", null, "keyword"),
        schema("concat(Text, 'bc')", null, "keyword"),
        schema("concat(TextWithFielddata)", null, "keyword"),
        schema("concat(TextWithFielddata, 'cd')", null, "keyword"),
        schema("concat(TextWithFields)", null, "keyword"),
        schema("concat(TextWithFields, 'de')", null, "keyword"),
        schema("concat(TextWithNumbers)", null, "keyword"),
        schema("concat(TextWithNumbers, 'ef')", null, "keyword"),
        schema("concat(TextWithMixedFields)", null, "keyword"),
        schema("concat(TextWithMixedFields, 'fg')", null, "keyword"));
    verifyDataRows(
        result,
        rows(
            "K for Keyword",
            "K for Keywordab",
            "Text With Keywords",
            "Text With Keywordsbc",
            null,
            null,
            "Text",
            "Textcd",
            null,
            null,
            null,
            null,
            "one two tree",
            "one two treefg"),
        rows(
            null,
            null,
            null,
            null,
            "Text",
            "Textbc",
            "Fielddata is the best!",
            "Fielddata is the best!cd",
            "Fields for freedom",
            "Fields for freedomde",
            "42",
            "42ef",
            "42",
            "42fg"),
        rows(
            "ikiki",
            "ikikiab",
            "Another Text With Similar Keywords",
            "Another Text With Similar Keywordsbc",
            null,
            null,
            null,
            null,
            null,
            null,
            "42",
            "42ef",
            "12.04.1961",
            "12.04.1961fg"),
        rows(
            null,
            null,
            "",
            "bc",
            "Some text",
            "Some textbc",
            "Text With Fielddata",
            "Text With Fielddatacd",
            "Freedom for fields",
            "Freedom for fieldsde",
            "100500",
            "100500ef",
            null,
            null),
        rows(
            "kokoko",
            "kokokoab",
            null,
            null,
            "A fairy tale about little mermaid",
            "A fairy tale about little mermaidbc",
            null,
            null,
            "A Text with a Field",
            "A Text with a Fieldde",
            null,
            null,
            null,
            null));
  }

  @Test
  public void test_concat_ws_with_different_types() {
    JSONObject result =
        executeQuery(
            "select concat_ws(' ', Keyword, Keyword) AS `KW + KW`, "
                + "concat_ws(' ', TextWithKeywords, Keyword) AS `T_KW + KW`, "
                + "concat_ws('__', TextWithFielddata, TextWithFields) AS `T_FD + T_F`, "
                + "concat_ws('/', '|', TextWithNumbers) AS `U + U + T_N`, "
                + "concat_WS(' == ', TextWithMixedFields, TextWithNumbers) AS `T_MX + T_N` FROM "
                + TEST_INDEX_TEXT);

    verifySchema(
        result,
        schema("concat_ws(' ', Keyword, Keyword)", "KW + KW", "keyword"),
        schema("concat_ws(' ', TextWithKeywords, Keyword)", "T_KW + KW", "keyword"),
        schema("concat_ws('__', TextWithFielddata, TextWithFields)", "T_FD + T_F", "keyword"),
        schema("concat_ws('/', '|', TextWithNumbers)", "U + U + T_N", "keyword"),
        schema("concat_WS(' == ', TextWithMixedFields, TextWithNumbers)", "T_MX + T_N", "keyword"));
    verifyDataRows(
        result,
        rows("K for Keyword K for Keyword", "Text With Keywords K for Keyword", null, null, null),
        rows(null, null, "Fielddata is the best!__Fields for freedom", "|/42", "42 == 42"),
        rows(
            "ikiki ikiki",
            "Another Text With Similar Keywords ikiki",
            null,
            "|/42",
            "12.04.1961 == 42"),
        rows(null, null, "Text With Fielddata__Freedom for fields", "|/100500", null),
        rows("kokoko kokoko", null, null, null, null));
  }

  @Test
  public void test_simple_string_functions() {
    JSONObject result =
        executeQuery(
            "select left(Keyword, 4) as left, upper(TextWithKeywords) as upper, locate('t', Text)"
                + " as locate, trim(TextWithFielddata) as trim, reverse(TextWithFields) as reverse,"
                + " substring(TextWithNumbers, 5, 3) as substring, TextWithMixedFields = '42' as"
                + " compare FROM "
                + TEST_INDEX_TEXT);

    verifySchema(
        result,
        schema("left(Keyword, 4)", "left", "keyword"),
        schema("upper(TextWithKeywords)", "upper", "keyword"),
        schema("locate('t', Text)", "locate", "integer"),
        schema("trim(TextWithFielddata)", "trim", "keyword"),
        schema("reverse(TextWithFields)", "reverse", "keyword"),
        schema("substring(TextWithNumbers, 5, 3)", "substring", "keyword"),
        schema("TextWithMixedFields = '42'", "compare", "boolean"));
    verifyDataRows(
        result,
        rows("K fo", "TEXT WITH KEYWORDS", null, "Text", null, null, false),
        rows(null, null, 4, "Fielddata is the best!", "modeerf rof sdleiF", "", true),
        rows("ikik", "ANOTHER TEXT WITH SIMILAR KEYWORDS", null, null, null, "", false),
        rows(null, "", 6, "Text With Fielddata", "sdleif rof modeerF", "00", null),
        rows("koko", null, 9, null, "dleiF a htiw txeT A", null, null));
  }

  @Test
  public void test_cast_text_to_bool() {
    JSONObject result =
        executeQuery(
            String.format(
                "select "
                    + "cast(TextWithKeywords as BOOLEAN) as TextWithKeywords, "
                    + "cast(TextWithFielddata as BOOLEAN) as TextWithFielddata, "
                    + "cast(TextWithNumbers as BOOLEAN) as TextWithNumbers, "
                    + "cast(TextWithMixedFields as BOOLEAN) as TextWithMixedFields, "
                    + "cast(Text as BOOLEAN) as Text, "
                    + "cast(TextWithFields as BOOLEAN) as TextWithFields "
                    + "from %s where TestName = 'bool';",
                TEST_INDEX_TEXT_FOR_CAST));

    verifySchema(
        result,
        schema("cast(TextWithKeywords as BOOLEAN)", "TextWithKeywords", "boolean"),
        schema("cast(TextWithFielddata as BOOLEAN)", "TextWithFielddata", "boolean"),
        schema("cast(TextWithNumbers as BOOLEAN)", "TextWithNumbers", "boolean"),
        schema("cast(TextWithMixedFields as BOOLEAN)", "TextWithMixedFields", "boolean"),
        schema("cast(Text as BOOLEAN)", "Text", "boolean"),
        schema("cast(TextWithFields as BOOLEAN)", "TextWithFields", "boolean"));
    verifyDataRows(
        result,
        // int -> bool conversion doesn't fool C standard, so
        // TextWithNumbers's value 42 converted to `false`
        rows(true, true, false, true, true, true),
        rows(false, false, false, false, false, false));
  }

  @Test
  public void test_cast_text_to_int() {
    JSONObject result =
        executeQuery(
            String.format(
                "select "
                    + "cast(TextWithKeywords as INT) as TextWithKeywords, "
                    + "cast(TextWithFielddata as INT) as TextWithFielddata, "
                    + "cast(TextWithNumbers as INT) as TextWithNumbers, "
                    + "cast(TextWithMixedFields as INT) as TextWithMixedFields, "
                    + "cast(Text as INT) as Text, "
                    + "cast(TextWithFields as INT) as TextWithFields "
                    + "from %s where TestName = 'int';",
                TEST_INDEX_TEXT_FOR_CAST));

    verifySchema(
        result,
        schema("cast(TextWithKeywords as INT)", "TextWithKeywords", "integer"),
        schema("cast(TextWithFielddata as INT)", "TextWithFielddata", "integer"),
        schema("cast(TextWithNumbers as INT)", "TextWithNumbers", "integer"),
        schema("cast(TextWithMixedFields as INT)", "TextWithMixedFields", "integer"),
        schema("cast(Text as INT)", "Text", "integer"),
        schema("cast(TextWithFields as INT)", "TextWithFields", "integer"));
    verifyDataRows(result, rows(1, -14, 100500, 42, 0, 4096));
  }

  @Test
  public void test_cast_text_to_double() {
    JSONObject result =
        executeQuery(
            String.format(
                "select "
                    + "cast(TextWithKeywords as DOUBLE) as TextWithKeywords, "
                    + "cast(TextWithFielddata as DOUBLE) as TextWithFielddata, "
                    + "cast(TextWithNumbers as DOUBLE) as TextWithNumbers, "
                    + "cast(TextWithMixedFields as DOUBLE) as TextWithMixedFields, "
                    + "cast(Text as DOUBLE) as Text, "
                    + "cast(TextWithFields as DOUBLE) as TextWithFields "
                    + "from %s where TestName = 'double';",
                TEST_INDEX_TEXT_FOR_CAST));

    verifySchema(
        result,
        schema("cast(TextWithKeywords as DOUBLE)", "TextWithKeywords", "double"),
        schema("cast(TextWithFielddata as DOUBLE)", "TextWithFielddata", "double"),
        schema("cast(TextWithNumbers as DOUBLE)", "TextWithNumbers", "double"),
        schema("cast(TextWithMixedFields as DOUBLE)", "TextWithMixedFields", "double"),
        schema("cast(Text as DOUBLE)", "Text", "double"),
        schema("cast(TextWithFields as DOUBLE)", "TextWithFields", "double"));
    verifyDataRows(result, rows(1.0, 3.14, -333.0, 42.0, .0, .223));
  }

  @Test
  public void test_cast_text_to_date() {
    // TextWithNumbers is excluded from the test, because we can't ingest the data
    // Value "2011-11-22" for that field is not accepted, because OS tries to parse it as a number
    JSONObject result =
        executeQuery(
            String.format(
                "select "
                    + "cast(TextWithKeywords as DATE) as TextWithKeywords, "
                    + "cast(TextWithFielddata as DATE) as TextWithFielddata, "
                    + "cast(TextWithMixedFields as DATE) as TextWithMixedFields, "
                    + "cast(Text as DATE) as Text, "
                    + "cast(TextWithFields as DATE) as TextWithFields "
                    + "from %s where TestName = 'date';",
                TEST_INDEX_TEXT_FOR_CAST));

    verifySchema(
        result,
        schema("cast(TextWithKeywords as DATE)", "TextWithKeywords", "date"),
        schema("cast(TextWithFielddata as DATE)", "TextWithFielddata", "date"),
        schema("cast(TextWithMixedFields as DATE)", "TextWithMixedFields", "date"),
        schema("cast(Text as DATE)", "Text", "date"),
        schema("cast(TextWithFields as DATE)", "TextWithFields", "date"));
    verifyDataRows(
        result, rows("1984-04-04", "1999-10-20", "1977-07-17", "1961-04-12", "2020-02-29"));
  }

  @Test
  public void test_cast_text_to_time() {
    // TextWithNumbers is excluded from the test, because we can't ingest the data
    // Value "07:08:09" for that field is not accepted, because OS tries to parse it as a number
    JSONObject result =
        executeQuery(
            String.format(
                "select "
                    + "cast(TextWithKeywords as TIME) as TextWithKeywords, "
                    + "cast(TextWithFielddata as TIME) as TextWithFielddata, "
                    + "cast(TextWithMixedFields as TIME) as TextWithMixedFields, "
                    + "cast(Text as TIME) as Text, "
                    + "cast(TextWithFields as TIME) as TextWithFields "
                    + "from %s where TestName = 'time';",
                TEST_INDEX_TEXT_FOR_CAST));

    verifySchema(
        result,
        schema("cast(TextWithKeywords as TIME)", "TextWithKeywords", "time"),
        schema("cast(TextWithFielddata as TIME)", "TextWithFielddata", "time"),
        schema("cast(TextWithMixedFields as TIME)", "TextWithMixedFields", "time"),
        schema("cast(Text as TIME)", "Text", "time"),
        schema("cast(TextWithFields as TIME)", "TextWithFields", "time"));

    verifyDataRows(result, rows("22:22:00", "00:00:00", "07:17:00", "10:20:30", "00:00:01"));
  }

  @Test
  public void test_cast_text_to_timestamp() {
    // TextWithNumbers is excluded from the test for the same reasons (see above)
    JSONObject result =
        executeQuery(
            String.format(
                "select "
                    + "cast(TextWithKeywords as TIMESTAMP) as TextWithKeywords, "
                    + "cast(TextWithFielddata as TIMESTAMP) as TextWithFielddata, "
                    + "cast(TextWithMixedFields as TIMESTAMP) as TextWithMixedFields, "
                    + "cast(Text as TIMESTAMP) as Text, "
                    + "cast(TextWithFields as TIMESTAMP) as TextWithFields "
                    + "from %s where TestName = 'timestamp';",
                TEST_INDEX_TEXT_FOR_CAST));

    verifySchema(
        result,
        schema("cast(TextWithKeywords as TIMESTAMP)", "TextWithKeywords", "timestamp"),
        schema("cast(TextWithFielddata as TIMESTAMP)", "TextWithFielddata", "timestamp"),
        schema("cast(TextWithMixedFields as TIMESTAMP)", "TextWithMixedFields", "timestamp"),
        schema("cast(Text as TIMESTAMP)", "Text", "timestamp"),
        schema("cast(TextWithFields as TIMESTAMP)", "TextWithFields", "timestamp"));

    verifyDataRows(
        result,
        rows(
            "1984-04-04 10:20:30",
            "1999-10-20 00:01:02",
            "1977-07-17 01:02:03",
            "1961-04-12 09:07:00",
            "2020-02-29 00:00:00"));
  }

  @SneakyThrows
  protected JSONObject executeQuery(String query) {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
