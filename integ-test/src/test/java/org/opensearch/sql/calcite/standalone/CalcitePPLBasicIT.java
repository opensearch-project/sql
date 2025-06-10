/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.exception.SemanticCheckException;

public class CalcitePPLBasicIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();
    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);
    // PUT index test1
    Request request3 = new Request("PUT", "/test1/_doc/1?refresh=true");
    request3.setJsonEntity("{\"name\": \"HELLO\", \"alias\": \"Hello\"}");
    client().performRequest(request3);

    loadIndex(Index.BANK);
    loadIndex(Index.DATA_TYPE_ALIAS);
    loadIndex(Index.MERGE_TEST_1);
    loadIndex(Index.MERGE_TEST_2);
  }

  @Test
  public void testInvalidTable() {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> execute("source=unknown"));
    verifyErrorMessageContains(
        e, "OpenSearch exception [type=index_not_found_exception, reason=no such index [unknown]]");
  }

  @Test
  public void testSourceQuery() {
    JSONObject actual = executeQuery("source=test");
    verifySchema(actual, schema("name", "string"), schema("age", "long"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void testMultipleSourceQuery_SameTable() {
    JSONObject actual = executeQuery("source=test, test");
    verifySchema(actual, schema("name", "string"), schema("age", "long"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void testMultipleSourceQuery_DifferentTables() {
    JSONObject actual = executeQuery("source=test, test1");
    verifySchema(
        actual, schema("name", "string"), schema("age", "long"), schema("alias", "string"));
    verifyDataRows(
        actual, rows("hello", null, 20), rows("world", null, 30), rows("HELLO", "Hello", null));
  }

  @Test
  public void testIndexPatterns() {
    JSONObject actual = executeQuery("source=test*");
    verifySchema(
        actual, schema("name", "string"), schema("age", "long"), schema("alias", "string"));
    verifyDataRows(
        actual, rows("hello", null, 20), rows("world", null, 30), rows("HELLO", "Hello", null));
  }

  @Test
  public void testSourceFieldQuery() {
    JSONObject actual = executeQuery("source=test | fields name");
    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("hello"), rows("world"));
  }

  @Test
  public void testFieldsShouldBeCaseSensitive() {
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> execute("source=test | fields NAME"));
    verifyErrorMessageContains(
        e,
        "field [NAME] not found; input fields are: [name, age, _id, _index, _score, _maxscore,"
            + " _sort, _routing]");
  }

  @Test
  public void testFilterQuery1() {
    JSONObject actual = executeQuery("source=test | where age = 30 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "long"));
    verifyDataRows(actual, rows("world", 30));
  }

  @Test
  public void testFilterQuery2() {
    JSONObject actual = executeQuery("source=test | where age = 20 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "long"));
    verifyDataRows(actual, rows("hello", 20));
  }

  @Test
  public void testFilterQuery3() {
    JSONObject actual =
        executeQuery("source=test | where age > 10 AND age < 100 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "long"));
    verifyDataRows(actual, rows("hello", 20), rows("world", 30));
  }

  @Test
  public void testFilterQuery4() {
    JSONObject actual = executeQuery("source=test | where age = 20.0 | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "long"));
    verifyDataRows(actual, rows("hello", 20));
  }

  @Test
  public void testRegexpFilter() {
    JSONObject actual = executeQuery("source=test | where name REGEXP 'he.*' | fields name, age");
    verifySchema(actual, schema("name", "string"), schema("age", "long"));
    verifyDataRows(actual, rows("hello", 20));
  }

  @Test
  public void testFilterOnTextField() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where gender = 'F' | fields firstname, lastname", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
    verifyDataRows(
        actual, rows("Nanette", "Bates"), rows("Virginia", "Ayala"), rows("Dillard", "Mcpherson"));
  }

  @Test
  public void testFilterOnTextFieldWithKeywordSubField() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where state = 'VA' | fields firstname, lastname", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
    verifyDataRows(actual, rows("Nanette", "Bates"));
  }

  @Test
  public void testFilterQueryWithOr() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where (account_number = 20 or city = 'Brogan') and balance > 10000 |"
                    + " fields firstname, lastname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
    verifyDataRows(actual, rows("Amber JOHnny", "Duke Willmington"), rows("Elinor", "Ratliff"));
  }

  @Test
  public void testFilterQueryWithOr2() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s (account_number = 20 or city = 'Brogan') and balance > 10000 |"
                    + " fields firstname, lastname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("lastname", "string"));
    verifyDataRows(actual, rows("Amber JOHnny", "Duke Willmington"), rows("Elinor", "Ratliff"));
  }

  @Test
  public void testQueryMinusFields() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | fields - firstname, lastname, birthdate", TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("account_number", "long"),
        schema("address", "string"),
        schema("gender", "string"),
        schema("city", "string"),
        schema("balance", "long"),
        schema("employer", "string"),
        schema("state", "string"),
        schema("age", "integer"),
        schema("email", "string"),
        schema("male", "boolean"));
    verifyDataRows(
        actual,
        rows(
            1,
            "880 Holmes Lane",
            "M",
            "Brogan",
            39225,
            "Pyrami",
            "IL",
            32,
            "amberduke@pyrami.com",
            true),
        rows(
            6,
            "671 Bristol Street",
            "M",
            "Dante",
            5686,
            "Netagy",
            "TN",
            36,
            "hattiebond@netagy.com",
            true),
        rows(
            13,
            "789 Madison Street",
            "F",
            "Nogal",
            32838,
            "Quility",
            "VA",
            28,
            "nanettebates@quility.com",
            false),
        rows(
            18,
            "467 Hutchinson Court",
            "M",
            "Orick",
            4180,
            "Boink",
            "MD",
            33,
            "daleadams@boink.com",
            true),
        rows(
            20,
            "282 Kings Place",
            "M",
            "Ribera",
            16418,
            "Scentric",
            "WA",
            36,
            "elinorratliff@scentric.com",
            true),
        rows(
            25,
            "171 Putnam Avenue",
            "F",
            "Nicholson",
            40540,
            "Filodyne",
            "PA",
            39,
            "virginiaayala@filodyne.com",
            false),
        rows(
            32,
            "702 Quentin Street",
            "F",
            "Veguita",
            48086,
            "Quailcom",
            "IN",
            34,
            "dillardmcpherson@quailcom.com",
            false));
  }

  @Test
  public void testQueryMinusFieldsWithFilter() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where (account_number = 20 or city = 'Brogan') and balance > 10000 |"
                    + " fields - firstname, lastname",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("account_number", "long"),
        schema("address", "string"),
        schema("birthdate", "timestamp"),
        schema("gender", "string"),
        schema("city", "string"),
        schema("balance", "long"),
        schema("employer", "string"),
        schema("state", "string"),
        schema("age", "integer"),
        schema("email", "string"),
        schema("male", "boolean"));
    verifyDataRows(
        actual,
        rows(
            1,
            "880 Holmes Lane",
            "2017-10-23 00:00:00",
            "M",
            "Brogan",
            39225,
            "Pyrami",
            "IL",
            32,
            "amberduke@pyrami.com",
            true),
        rows(
            20,
            "282 Kings Place",
            "2018-06-27 00:00:00",
            "M",
            "Ribera",
            16418,
            "Scentric",
            "WA",
            36,
            "elinorratliff@scentric.com",
            true));
  }

  @Test
  public void testFieldsPlusThenMinus() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields + firstname, lastname, account_number | fields - firstname,"
                    + " lastname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("account_number", "long"));
    verifyDataRows(actual, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
  }

  @Test
  public void testMultipleTables_SameTable() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s, %s | stats count() as c", TEST_INDEX_BANK, TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "long"));
    verifyDataRows(actual, rows(7));
  }

  @Test
  public void testMultipleTablesAndFilters_SameTable() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s, %s gender = 'F' | stats count() as c",
                TEST_INDEX_BANK, TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "long"));
    verifyDataRows(actual, rows(3));
  }

  @Test
  public void testMultipleTables_DifferentTables() {
    JSONObject actual =
        executeQuery(String.format("source=%s, test | stats count() as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "long"));
    verifyDataRows(actual, rows(9));
  }

  @Test
  public void testMultipleTables_WithIndexPattern() {
    JSONObject actual =
        executeQuery(String.format("source=%s, test* | stats count() as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "long"));
    verifyDataRows(actual, rows(10));
  }

  @Test
  public void testMultipleTablesAndFilters_WithIndexPattern() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s, test* gender = 'F' | stats count() as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "long"));
    verifyDataRows(actual, rows(3));
  }

  @Test
  public void testSelectDateTypeField() {
    JSONObject actual =
        executeQuery(String.format("source=%s | fields birthdate", TEST_INDEX_BANK));
    verifySchema(actual, schema("birthdate", "timestamp"));
    verifyDataRows(
        actual,
        rows("2017-10-23 00:00:00"),
        rows("2017-11-20 00:00:00"),
        rows("2018-06-23 00:00:00"),
        rows("2018-11-13 23:33:20"),
        rows("2018-06-27 00:00:00"),
        rows("2018-08-19 00:00:00"),
        rows("2018-08-11 00:00:00"));
  }

  @Test
  public void testAllFieldsInTable() throws IOException {
    Request request = new Request("PUT", "/a/_doc/1?refresh=true");
    request.setJsonEntity("{\"name\": \"hello\"}");
    client().performRequest(request);

    JSONObject actual = executeQuery("source=a | fields name");
    verifySchema(actual, schema("name", "string"));
    verifyDataRows(actual, rows("hello"));
  }

  @Test
  public void testBetween() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age between 35 and 38 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testBetweenWithExpression() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age between 36 - 1 and 37 + 1 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testBetweenWithDifferentTypes() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age between 35.5 and 38.5 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testBetweenWithDifferentTypes2() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age between 35 and 38.5 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  @Test
  public void testBetweenWithIncompatibleTypes() {
    SemanticCheckException e =
        assertThrows(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | where age between '35' and 38.5 | fields firstname, age",
                        TEST_INDEX_BANK)));
    verifyErrorMessageContains(e, "BETWEEN expression types are incompatible");
  }

  @Test
  public void testNotBetween() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where age not between 30 and 39 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Nanette", 28));
  }

  @Test
  public void testNotBetween2() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where not age between 30 and 39 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Nanette", 28));
  }

  @Test
  public void testNotBetween3() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where not age not between 35 and 38 | fields firstname, age",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(actual, rows("Hattie", 36), rows("Elinor", 36));
  }

  public void testDateBetween() {
    JSONObject actual =
        executeQuery(
            String.format(
                """
                    source=%s
                    | where birthdate between date('2018-06-01') and date('2018-06-30')
                    | fields firstname, birthdate
                    """,
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("birthdate", "timestamp"));
    verifyDataRows(
        actual, rows("Nanette", "2018-06-23 00:00:00"), rows("Elinor", "2018-06-27 00:00:00"));
  }

  @Test
  public void testXor() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where firstname='Hattie' xor age=36 | fields firstname, age",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Elinor", 36));
  }

  @Test
  public void testKeepThrowCalciteException() throws IOException {
    withFallbackEnabled(
        () -> {
          IllegalArgumentException e =
              assertThrows(
                  IllegalArgumentException.class,
                  () ->
                      executeQuery(
                          String.format("source=%s | fields firstname1, age", TEST_INDEX_BANK)));
          verifyErrorMessageContains(
              e,
              "field [firstname1] not found; input fields are: [account_number, firstname, address,"
                  + " birthdate, gender, city, lastname, balance, employer, state, age, email,"
                  + " male, _id, _index, _score, _maxscore, _sort, _routing]");
        },
        "");
  }

  @Test
  public void testAliasDataType() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | where alias_col > 1 | fields original_col, alias_col ",
                TEST_INDEX_ALIAS));
    verifySchema(result, schema("original_col", "integer"), schema("alias_col", "integer"));
    verifyDataRows(result, rows(2, 2), rows(3, 3));
  }

  @Test
  public void testMetaFieldAlias() {
    Exception e =
        assertThrows(
            Exception.class,
            () ->
                executeQuery(
                    String.format("source=%s | stats count() as _score", TEST_INDEX_ACCOUNT)));
    verifyErrorMessageContains(e, "Cannot use metadata field [_score] as the alias.");
  }

  @Test
  public void testFieldsMergedObject() {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | fields machine.os1,  machine.os2, machine_array.os1, "
                    + " machine_array.os2, machine_deep.attr1, machine_deep.attr2,"
                    + " machine_deep.layer.os1, machine_deep.layer.os2",
                TEST_INDEX_MERGE_TEST_WILDCARD));
    verifySchema(
        result,
        schema("machine.os1", "string"),
        schema("machine.os2", "string"),
        schema("machine_array.os1", "string"),
        schema("machine_array.os2", "string"),
        schema("machine_deep.attr1", "long"),
        schema("machine_deep.attr2", "long"),
        schema("machine_deep.layer.os1", "string"),
        schema("machine_deep.layer.os2", "string"));
    verifyDataRows(
        result,
        rows("linux", null, "linux", null, 1, null, "os1", null),
        rows(null, "linux", null, "linux", null, 2, null, "os2"));
  }

  public void testNumericLiteral() {
    JSONObject result =
        executeQuery(
            "source=test | eval decimalLiteral = 0.06 - 0.01, doubleLiteral = 0.06d - 0.01d,"
                + " floatLiteral = 0.06f - 0.01f");
    verifySchema(
        result,
        schema("name", "string"),
        schema("age", "long"),
        schema("decimalLiteral", "double"),
        schema("doubleLiteral", "double"),
        schema("floatLiteral", "float"));
    verifyDataRows(
        result,
        rows("hello", 20, 0.05, 0.049999999999999996, 0.05),
        rows("world", 30, 0.05, 0.049999999999999996, 0.05));
  }

  @Test
  public void testDecimalLiteral() {
    JSONObject result =
        executeQuery(
            "source=test | eval r1 = 22 / 7.0, r2 = 22 / 7.0d, r3 = 22.0 / 7, r4 = 22.0d / 7,"
                + " r5 = 0.1 * 0.2, r6 = 0.1d * 0.2d, r7 = 0.1 + 0.2, r8 = 0.1d + 0.2d,"
                + " r9 = 0.06 - 0.01, r10 = 0.06d - 0.01d, r11 = 0.1 / 0.3 * 0.3,"
                + " r12 = 0.1d / 0.3d * 0.3d, r13 = pow(sqrt(2.0), 2), r14 = pow(sqrt(2.0d), 2),"
                + " r15 = 7.0 / 0, r16 = 7 / 0.0");
    verifyDataRows(
        result,
        rows(
            "hello",
            20,
            3.142857142857143,
            3.142857142857143,
            3.142857142857143,
            3.142857142857143,
            0.02,
            0.020000000000000004,
            0.3,
            0.30000000000000004,
            0.05,
            0.049999999999999996,
            0.1,
            0.1,
            2.0000000000000004,
            2.0000000000000004,
            null,
            null),
        rows(
            "world",
            30,
            3.142857142857143,
            3.142857142857143,
            3.142857142857143,
            3.142857142857143,
            0.02,
            0.020000000000000004,
            0.3,
            0.30000000000000004,
            0.05,
            0.049999999999999996,
            0.1,
            0.1,
            2.0000000000000004,
            2.0000000000000004,
            null,
            null));
  }
}
