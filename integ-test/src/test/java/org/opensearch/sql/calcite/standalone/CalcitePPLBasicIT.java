/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

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

    Request request3 = new Request("PUT", "/test_name_null/_doc/1?refresh=true");
    request3.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request3);
    Request request4 = new Request("PUT", "/test_name_null/_doc/2?refresh=true");
    request4.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request4);
    Request request5 = new Request("PUT", "/test_name_null/_doc/3?refresh=true");
    request5.setJsonEntity("{\"name\": null, \"age\": 30}");
    client().performRequest(request5);

    Request request6 = new Request("PUT", "/people/_doc/2?refresh=true");
    request6.setJsonEntity("{\"name\": \"DummyEntityForMathVerification\", \"age\": 24}");
    client().performRequest(request6);

    loadIndex(Index.BANK);
  }

  @Test
  public void testInvalidTable() {
    assertThrows(
        "OpenSearch exception [type=index_not_found_exception, reason=no such index [unknown]]",
        IllegalStateException.class,
        () -> execute("source=unknown"));
  }

  @Ignore
  public void testTakeAggregation() {
    String actual = execute("source=test | stats take(name, 2)");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"take(name, 2)\",\n"
            + "      \"type\": \"array\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      [\"hello\", \"world\"]\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testSourceQuery() {
    String actual = execute("source=test");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"world\",\n"
            + "      30\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Test
  public void testMultipleSourceQuery() {
    String actual = execute("source=test, test");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"world\",\n"
            + "      30\n"
            + "    ],\n"
            + "    [\n"
            + "      \"hello\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"world\",\n"
            + "      30\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 4,\n"
            + "  \"size\": 4\n"
            + "}",
        actual);
  }

  @Test
  public void testSourceFieldQuery() {
    String actual = execute("source=test | fields name");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"world\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Test
  public void testFilterQuery1() {
    String actual = execute("source=test | where age = 30 | fields name, age");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"world\",\n"
            + "      30\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testFilterQuery2() {
    String actual = execute("source=test | where age = 20 | fields name, age");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\",\n"
            + "      20\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testFilterQuery3() {
    String actual = execute("source=test | where age > 10 AND age < 100 | fields name, age");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"world\",\n"
            + "      30\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  // TODO fail after merged https://github.com/opensearch-project/sql/pull/3327
  @Ignore
  @Test
  public void testFilterQueryWithOr() {
    String actual =
        execute(
            String.format(
                "source=%s | where (account_number = 25 or balance > 10000) and gender = 'M' |"
                    + " fields firstname, lastname",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"lastname\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Amber JOHnny\",\n"
            + "      \"Duke Willmington\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Elinor\",\n"
            + "      \"Ratliff\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  // TODO fail after merged https://github.com/opensearch-project/sql/pull/3327
  @Ignore
  @Test
  public void testFilterQueryWithOr2() {
    String actual =
        execute(
            String.format(
                "source=%s (account_number = 25 or balance > 10000) and gender = 'M' |"
                    + " fields firstname, lastname",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"lastname\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Amber JOHnny\",\n"
            + "      \"Duke Willmington\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Elinor\",\n"
            + "      \"Ratliff\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Test
  public void testQueryMinusFields() {
    String actual =
        execute(
            String.format("source=%s | fields - firstname, lastname, birthdate", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"account_number\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"address\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"city\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"balance\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"employer\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"email\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"male\",\n"
            + "      \"type\": \"boolean\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      1,\n"
            + "      \"880 Holmes Lane\",\n"
            + "      \"M\",\n"
            + "      \"Brogan\",\n"
            + "      39225,\n"
            + "      \"Pyrami\",\n"
            + "      \"IL\",\n"
            + "      32,\n"
            + "      \"amberduke@pyrami.com\",\n"
            + "      true\n"
            + "    ],\n"
            + "    [\n"
            + "      6,\n"
            + "      \"671 Bristol Street\",\n"
            + "      \"M\",\n"
            + "      \"Dante\",\n"
            + "      5686,\n"
            + "      \"Netagy\",\n"
            + "      \"TN\",\n"
            + "      36,\n"
            + "      \"hattiebond@netagy.com\",\n"
            + "      true\n"
            + "    ],\n"
            + "    [\n"
            + "      13,\n"
            + "      \"789 Madison Street\",\n"
            + "      \"F\",\n"
            + "      \"Nogal\",\n"
            + "      32838,\n"
            + "      \"Quility\",\n"
            + "      \"VA\",\n"
            + "      28,\n"
            + "      \"nanettebates@quility.com\",\n"
            + "      false\n"
            + "    ],\n"
            + "    [\n"
            + "      18,\n"
            + "      \"467 Hutchinson Court\",\n"
            + "      \"M\",\n"
            + "      \"Orick\",\n"
            + "      4180,\n"
            + "      \"Boink\",\n"
            + "      \"MD\",\n"
            + "      33,\n"
            + "      \"daleadams@boink.com\",\n"
            + "      true\n"
            + "    ],\n"
            + "    [\n"
            + "      20,\n"
            + "      \"282 Kings Place\",\n"
            + "      \"M\",\n"
            + "      \"Ribera\",\n"
            + "      16418,\n"
            + "      \"Scentric\",\n"
            + "      \"WA\",\n"
            + "      36,\n"
            + "      \"elinorratliff@scentric.com\",\n"
            + "      true\n"
            + "    ],\n"
            + "    [\n"
            + "      25,\n"
            + "      \"171 Putnam Avenue\",\n"
            + "      \"F\",\n"
            + "      \"Nicholson\",\n"
            + "      40540,\n"
            + "      \"Filodyne\",\n"
            + "      \"PA\",\n"
            + "      39,\n"
            + "      \"virginiaayala@filodyne.com\",\n"
            + "      false\n"
            + "    ],\n"
            + "    [\n"
            + "      32,\n"
            + "      \"702 Quentin Street\",\n"
            + "      \"F\",\n"
            + "      \"Veguita\",\n"
            + "      48086,\n"
            + "      \"Quailcom\",\n"
            + "      \"IN\",\n"
            + "      34,\n"
            + "      \"dillardmcpherson@quailcom.com\",\n"
            + "      false\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}",
        actual);
  }

  // TODO bug: shouldn't return empty
  @Ignore
  public void testQueryMinusFieldsWithFilter() {
    String actual =
        execute(
            String.format(
                "source=%s | where (account_number = 25 or balance > 10000) and gender = 'M' |"
                    + " fields - firstname, lastname",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"account_number\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"address\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"birthdate\",\n"
            + "      \"type\": \"timestamp\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"city\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"balance\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"employer\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"email\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"male\",\n"
            + "      \"type\": \"boolean\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [],\n"
            + "  \"total\": 0,\n"
            + "  \"size\": 0\n"
            + "}",
        actual);
  }

  @Test
  public void testFieldsPlusThenMinus() {
    String actual =
        execute(
            String.format(
                "source=%s | fields + firstname, lastname, account_number | fields - firstname,"
                    + " lastname",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"account_number\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      1\n"
            + "    ],\n"
            + "    [\n"
            + "      6\n"
            + "    ],\n"
            + "    [\n"
            + "      13\n"
            + "    ],\n"
            + "    [\n"
            + "      18\n"
            + "    ],\n"
            + "    [\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      25\n"
            + "    ],\n"
            + "    [\n"
            + "      32\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}",
        actual);
  }

  // TODO fail after merged https://github.com/opensearch-project/sql/pull/3327
  @Ignore
  @Test
  public void testMultipleTables() {
    String actual =
        execute(
            String.format("source=%s, %s | stats count() as c", TEST_INDEX_BANK, TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"c\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      14\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  // TODO fail after merged https://github.com/opensearch-project/sql/pull/3327
  @Ignore
  @Test
  public void testMultipleTablesAndFilters() {
    String actual =
        execute(
            String.format(
                "source=%s, %s gender = 'F' | stats count() as c",
                TEST_INDEX_BANK, TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"c\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      6\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testSelectDateTypeField() {
    String actual = execute(String.format("source=%s | fields birthdate", TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"birthdate\",\n"
            + "      \"type\": \"timestamp\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"2017-10-23 00:00:00\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"2017-11-20 00:00:00\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"2018-06-23 00:00:00\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"2018-11-13 23:33:20\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"2018-06-27 00:00:00\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"2018-08-19 00:00:00\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"2018-08-11 00:00:00\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}",
        actual);
  }

  @Test
  public void testAllFieldsInTable() throws IOException {
    Request request = new Request("PUT", "/a/_doc/1?refresh=true");
    request.setJsonEntity("{\"name\": \"hello\"}");
    client().performRequest(request);

    String actual = execute("source=a | fields name");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Ignore
  @Test
  public void testDate() {
    String query =
        "source=test |eval `DATE('2020-08-26')` = DATE('2020-08-26') | fields `DATE('2020-08-26')`";
    testSimplePPL(query, List.of("2020-08-26 01:00:00", "2020-08-27 01:01:01"));
  }

  @Ignore
  @Test
  public void testDateAdd() {
    String query =
        "source=test | eval `'2020-08-26' + 1h` = DATE_ADD(DATE('2020-08-26'), INTERVAL 1 HOUR),"
            + " `ts '2020-08-26 01:01:01' + 1d` = DATE_ADD(TIMESTAMP('2020-08-26 01:01:01'),"
            + " INTERVAL 1 DAY) | fields `'2020-08-26' + 1h`, `ts '2020-08-26 01:01:01' + 1d`";
    testSimplePPL(query, List.of("2020-08-26 01:00:00", "2020-08-27 01:01:01"));
  }

  @Test
  public void testConcat() {
    String query =
        "source=test | eval `CONCAT('hello', 'world')` = CONCAT('hello', 'world'),"
            + " `CONCAT('hello ', 'whole ', 'world', '!')` = CONCAT('a', 'b ', 'c', 'd', 'e',"
            + " 'f', 'g', '1', '2') | fields `CONCAT('hello', 'world')`, `CONCAT('hello ',"
            + " 'whole ', 'world', '!')`";

    testSimplePPL(query, List.of("helloworld", "ab cdefg12"));
  }

  @Test
  public void testConcatWs() {
    String query =
        "source=test | eval `CONCAT_WS(',', 'hello', 'world')` = CONCAT_WS(',', 'hello',"
            + " 'world') | fields `CONCAT_WS(',', 'hello', 'world')`";
    testSimplePPL(query, List.of("hello,world"));
  }

  @Test
  public void testLength() {
    String query =
        "source=test | eval `LENGTH('helloworld')` = LENGTH('helloworld') | fields"
            + " `LENGTH('helloworld')`";
    testSimplePPL(query, List.of(10));
  }

  @Test
  public void testLower() {
    String query =
        "source=test | eval `LOWER('helloworld')` = LOWER('helloworld'), `LOWER('HELLOWORLD')`"
            + " = LOWER('HELLOWORLD') | fields `LOWER('helloworld')`, `LOWER('HELLOWORLD')`";
    testSimplePPL(query, List.of("helloworld", "helloworld"));
  }

  @Test
  public void testLtrim() {
    String query =
        "source=test | eval `LTRIM('   hello')` = LTRIM('   hello'), `LTRIM('hello   ')` ="
            + " LTRIM('hello   ') | fields `LTRIM('   hello')`, `LTRIM('hello   ')`";
    testSimplePPL(query, List.of("hello", "hello   "));
  }

  @Test
  public void testPosition() {
    String query =
        "source=test | eval `POSITION('world' IN 'helloworld')` = POSITION('world' IN"
            + " 'helloworld'), `POSITION('invalid' IN 'helloworld')`= POSITION('invalid' IN"
            + " 'helloworld')  | fields `POSITION('world' IN 'helloworld')`, `POSITION('invalid' IN"
            + " 'helloworld')`";
    testSimplePPL(query, List.of(6, 0));
  }

  @Test
  public void testReverse() {
    String query =
        "source=test | eval `REVERSE('abcde')` = REVERSE('abcde') | fields `REVERSE('abcde')`";
    testSimplePPL(query, List.of("edcba"));
  }

  // @Ignore
  @Test
  public void testRight() {
    List<Object> expected = new ArrayList<>();
    expected.add("world");
    expected.add("");
    String query =
        "source=test | eval `RIGHT('helloworld', 5)` = RIGHT('helloworld', 5), `RIGHT('HELLOWORLD',"
            + " 0)` = RIGHT('HELLOWORLD', 0) | fields `RIGHT('helloworld', 5)`,"
            + " `RIGHT('HELLOWORLD', 0)`";
    testSimplePPL(query, expected);
  }

  @Test
  public void testLike() {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, '\\\\_test wildcard%') | fields KeywordBody";
  }

  @Test
  public void testRtrim() {
    String query =
        "source=test | eval `RTRIM('   hello')` = RTRIM('   hello'), `RTRIM('hello   ')` ="
            + " RTRIM('hello   ') | fields `RTRIM('   hello')`, `RTRIM('hello   ')`";
    testSimplePPL(query, List.of("   hello", "hello"));
  }

  @Test
  public void testSubstring() {
    String query =
        "source=test | eval `SUBSTRING('helloworld', 5)` = SUBSTRING('helloworld', 5),"
            + " `SUBSTRING('helloworld', 5, 3)` = SUBSTRING('helloworld', 5, 3) | fields"
            + " `SUBSTRING('helloworld', 5)`, `SUBSTRING('helloworld', 5, 3)`";
    testSimplePPL(query, List.of("oworld", "owo"));
  }

  @Test
  public void testTrim() {
    String query =
        "source=test | eval `TRIM('   hello')` = TRIM('   hello'), `TRIM('hello   ')` = TRIM('hello"
            + "   ') | fields `TRIM('   hello')`, `TRIM('hello   ')`";
    testSimplePPL(query, List.of("hello", "hello"));
  }

  @Test
  public void testUpper() {
    String query =
        "source=test | eval `UPPER('helloworld')` = UPPER('helloworld'), `UPPER('HELLOWORLD')` ="
            + " UPPER('HELLOWORLD') | fields `UPPER('helloworld')`, `UPPER('HELLOWORLD')`";
    testSimplePPL(query, List.of("HELLOWORLD", "HELLOWORLD"));
  }

  @Test
  public void testIf() {
    String actual =
        execute(
            "source=test_name_null | eval result = if(like(name, '%he%'), 'default', name) | fields"
                + " result");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"result\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"default\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"default\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"default\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 3,\n"
            + "  \"size\": 3\n"
            + "}",
        actual);
  }

  @Test
  public void testIfNull() {
    String actual =
        execute(
            "source=test_name_null | eval defaultName=ifnull(name, 'default') | fields"
                + " defaultName");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"defaultName\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"world\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"default\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 3,\n"
            + "  \"size\": 3\n"
            + "}",
        actual);
  }

  @Test
  public void testNullIf() {
    String actual =
        execute(
            "source=test_name_null | eval defaultName=nullif(name, 'world') | fields defaultName");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"defaultName\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 3,\n"
            + "  \"size\": 3\n"
            + "}",
        actual);
  }

  private static JsonArray parseAndGetFirstDataRow(String executionResult) {
    JsonObject sqrtResJson = JsonParser.parseString(executionResult).getAsJsonObject();
    JsonArray dataRows = sqrtResJson.getAsJsonArray("datarows");
    return dataRows.get(0).getAsJsonArray();
  }

  private void testSimplePPL(String query, List<Object> expectedValues) {
    String execResult = execute(query);
    JsonArray dataRow = parseAndGetFirstDataRow(execResult);
    assertEquals(expectedValues.size(), dataRow.size());
    for (int i = 0; i < expectedValues.size(); i++) {
      Object expected = expectedValues.get(i);
      if (Objects.isNull(expected)) {
        Object actual = dataRow.get(i);
        assertNull(actual);
      } else if (expected instanceof BigDecimal) {
        Number actual = dataRow.get(i).getAsNumber();
        assertEquals(expected, actual);
      } else if (expected instanceof Double || expected instanceof Float) {
        Number actual = dataRow.get(i).getAsNumber();
        assertDoubleUlpEquals(((Number) expected).doubleValue(), actual.doubleValue(), 8);
      } else if (expected instanceof Long || expected instanceof Integer) {
        Number actual = dataRow.get(i).getAsNumber();
        assertEquals(((Number) expected).longValue(), actual.longValue());
      } else if (expected instanceof String) {
        String actual = dataRow.get(i).getAsString();
        assertEquals(expected, actual);
      } else if (expected instanceof Boolean) {
        Boolean actual = dataRow.get(i).getAsBoolean();
        assertEquals(expected, actual);
      } else {
        fail("Unsupported number type: " + expected.getClass().getName());
      }
    }
  }

  @Test
  public void testAbs() {
    String absPpl = "source=people | eval `ABS(-1)` = ABS(-1) | fields `ABS(-1)`";
    List<Object> expected = List.of(1);
    testSimplePPL(absPpl, expected);
  }

  @Test
  public void testAcos() {
    String acosPpl = "source=people | eval `ACOS(0)` = ACOS(0) | fields `ACOS(0)`";
    List<Object> expected = List.of(Math.PI / 2);
    testSimplePPL(acosPpl, expected);
  }

  @Test
  public void testAsin() {
    String asinPpl = "source=people | eval `ASIN(0)` = ASIN(0) | fields `ASIN(0)`";
    List<Object> expected = List.of(0.0);
    testSimplePPL(asinPpl, expected);
  }

  @Test
  public void testAtan() {
    // TODO: Error while preparing plan [LogicalProject(ATAN(2)=[ATAN(2)], ATAN(2, 3)=[ATAN(2, 3)])
    // ATAN defined in OpenSearch accepts single and double arguments, while that defined in SQL
    // standard library accepts only single argument.
    testSimplePPL(
        "source=people | eval `ATAN(2)` = ATAN(2), `ATAN(2, 3)` = ATAN(2, 3) | fields `ATAN(2)`,"
            + " `ATAN(2, 3)`",
        List.of(Math.atan(2), Math.atan2(2, 3)));
  }

  @Test
  public void testAtan2() {
    testSimplePPL(
        "source=people | eval `ATAN2(2, 3)` = ATAN2(2, 3) | fields `ATAN2(2, 3)`",
        List.of(Math.atan2(2, 3)));
  }

  @Test
  public void testCeiling() {
    testSimplePPL(
        "source=people | eval `CEILING(0)` = CEILING(0), `CEILING(50.00005)` = CEILING(50.00005),"
            + " `CEILING(-50.00005)` = CEILING(-50.00005) | fields `CEILING(0)`,"
            + " `CEILING(50.00005)`, `CEILING(-50.00005)`",
        List.of(Math.ceil(0.0), Math.ceil(50.00005), Math.ceil(-50.00005)));
    testSimplePPL(
        "source=people | eval `CEILING(3147483647.12345)` = CEILING(3147483647.12345),"
            + " `CEILING(113147483647.12345)` = CEILING(113147483647.12345),"
            + " `CEILING(3147483647.00001)` = CEILING(3147483647.00001) | fields"
            + " `CEILING(3147483647.12345)`, `CEILING(113147483647.12345)`,"
            + " `CEILING(3147483647.00001)`",
        List.of(
            Math.ceil(3147483647.12345),
            Math.ceil(113147483647.12345),
            Math.ceil(3147483647.00001)));
  }

  @Ignore
  @Test
  public void testConv() {
    // TODO: Error while preparing plan [LogicalProject(CONV('12', 10, 16)=[CONVERT('12', 10, 16)],
    // CONV('2C', 16, 10)=[CONVERT('2C', 16, 10)], CONV(12, 10, 2)=[CONVERT(12, 10, 2)], CONV(1111,
    // 2, 10)=[CONVERT(1111, 2, 10)])
    //  OpenSearchTableScan(table=[[OpenSearch, people]])
    String convPpl =
        "source=people | eval `CONV('12', 10, 16)` = CONV('12', 10, 16), `CONV('2C', 16, 10)` ="
            + " CONV('2C', 16, 10), `CONV(12, 10, 2)` = CONV(12, 10, 2), `CONV(1111, 2, 10)` ="
            + " CONV(1111, 2, 10) | fields `CONV('12', 10, 16)`, `CONV('2C', 16, 10)`, `CONV(12,"
            + " 10, 2)`, `CONV(1111, 2, 10)`";
    String execResult = execute(convPpl);
    JsonArray dataRow = parseAndGetFirstDataRow(execResult);
    assertEquals(4, dataRow.size());
    assertEquals("c", dataRow.get(0).getAsString());
    assertEquals("44", dataRow.get(1).getAsString());
    assertEquals("1100", dataRow.get(2).getAsString());
    assertEquals("15", dataRow.get(3).getAsString());
  }

  @Test
  public void testCos() {
    testSimplePPL("source=people | eval `COS(0)` = COS(0) | fields `COS(0)`", List.of(1.0));
  }

  @Test
  public void testCot() {
    testSimplePPL(
        "source=people | eval `COT(1)` = COT(1) | fields `COT(1)`", List.of(1.0 / Math.tan(1)));
  }

  @Test
  public void testCrc32() {
    // TODO: No corresponding built-in implementation
    testSimplePPL(
        "source=people | eval `CRC32('MySQL')` = CRC32('MySQL') | fields `CRC32('MySQL')`",
        List.of(3259397556L));
  }

  @Test
  public void testDegrees() {
    testSimplePPL(
        "source=people | eval `DEGREES(1.57)` = DEGREES(1.57) | fields `DEGREES(1.57)`",
        List.of(Math.toDegrees(1.57)));
  }

  @Test
  public void testEuler() {
    // TODO: No corresponding built-in implementation
    testSimplePPL("source=people | eval `E()` = E() | fields `E()`", List.of(Math.E));
  }

  @Test
  public void testExp() {
    testSimplePPL("source=people | eval `EXP(2)` = EXP(2) | fields `EXP(2)`", List.of(Math.exp(2)));
  }

  @Test
  public void testFloor() {
    testSimplePPL(
        "source=people | eval `FLOOR(0)` = FLOOR(0), `FLOOR(50.00005)` = FLOOR(50.00005),"
            + " `FLOOR(-50.00005)` = FLOOR(-50.00005) | fields `FLOOR(0)`, `FLOOR(50.00005)`,"
            + " `FLOOR(-50.00005)`",
        List.of(Math.floor(0.0), Math.floor(50.00005), Math.floor(-50.00005)));
    testSimplePPL(
        "source=people | eval `FLOOR(3147483647.12345)` = FLOOR(3147483647.12345),"
            + " `FLOOR(113147483647.12345)` = FLOOR(113147483647.12345), `FLOOR(3147483647.00001)`"
            + " = FLOOR(3147483647.00001) | fields `FLOOR(3147483647.12345)`,"
            + " `FLOOR(113147483647.12345)`, `FLOOR(3147483647.00001)`",
        List.of(
            Math.floor(3147483647.12345),
            Math.floor(113147483647.12345),
            Math.floor(3147483647.00001)));
    testSimplePPL(
        "source=people | eval `FLOOR(282474973688888.022)` = FLOOR(282474973688888.022),"
            + " `FLOOR(9223372036854775807.022)` = FLOOR(9223372036854775807.022),"
            + " `FLOOR(9223372036854775807.0000001)` = FLOOR(9223372036854775807.0000001) | fields"
            + " `FLOOR(282474973688888.022)`, `FLOOR(9223372036854775807.022)`,"
            + " `FLOOR(9223372036854775807.0000001)`",
        List.of(
            Math.floor(282474973688888.022),
            Math.floor(9223372036854775807.022),
            Math.floor(9223372036854775807.0000001)));
  }

  @Test
  public void testLn() {
    testSimplePPL("source=people | eval `LN(2)` = LN(2) | fields `LN(2)`", List.of(Math.log(2)));
  }

  @Test
  public void testLog() {
    // TODO: No built-in function for 2-operand log
    testSimplePPL(
        "source=people | eval `LOG(2)` = LOG(2), `LOG(2, 8)` = LOG(2, 8) | fields `LOG(2)`, `LOG(2,"
            + " 8)`",
        List.of(Math.log(2), Math.log(8) / Math.log(2)));
  }

  @Test
  public void testLog2() {
    testSimplePPL(
        "source=people | eval `LOG2(8)` = LOG2(8) | fields `LOG2(8)`",
        List.of(Math.log(8) / Math.log(2)));
  }

  @Test
  public void testLog10() {
    testSimplePPL(
        "source=people | eval `LOG10(100)` = LOG10(100) | fields `LOG10(100)`",
        List.of(Math.log10(100)));
  }

  @Test
  public void testMod() {
    // TODO: There is a difference between MOD in OpenSearch and SQL standard library
    // For MOD in Calcite, MOD(3.1, 2) = 1
    testSimplePPL(
        "source=people | eval `MOD(3, 2)` = MOD(3, 2), `MOD(3.1, 2)` = MOD(3.1, 2) | fields `MOD(3,"
            + " 2)`, `MOD(3.1, 2)`",
        List.of(1, 1.1));
  }

  @Test
  public void testPi() {
    testSimplePPL("source=people | eval `PI()` = PI() | fields `PI()`", List.of(Math.PI));
  }

  @Test
  public void testPowAndPower() {
    testSimplePPL(
        "source=people | eval `POW(3, 2)` = POW(3, 2), `POW(-3, 2)` = POW(-3, 2), `POW(3, -2)` ="
            + " POW(3, -2) | fields `POW(3, 2)`, `POW(-3, 2)`, `POW(3, -2)`",
        List.of(Math.pow(3, 2), Math.pow(-3, 2), Math.pow(3, -2)));
    testSimplePPL(
        "source=people | eval `POWER(3, 2)` = POWER(3, 2), `POWER(-3, 2)` = POWER(-3, 2), `POWER(3,"
            + " -2)` = POWER(3, -2) | fields `POWER(3, 2)`, `POWER(-3, 2)`, `POWER(3, -2)`",
        List.of(Math.pow(3, 2), Math.pow(-3, 2), Math.pow(3, -2)));
  }

  @Test
  public void testRadians() {
    testSimplePPL(
        "source=people | eval `RADIANS(90)` = RADIANS(90) | fields `RADIANS(90)`",
        List.of(Math.toRadians(90)));
  }

  @Test
  public void testRand() {
    String randPpl = "source=people | eval `RAND(3)` = RAND(3) | fields `RAND(3)`";
    String execResult1 = execute(randPpl);
    String execResult2 = execute(randPpl);
    assertEquals(execResult1, execResult2);
    double val = parseAndGetFirstDataRow(execResult1).get(0).getAsDouble();
    assertTrue(val >= 0 && val <= 1);
  }

  @Test
  public void testRound() {
    testSimplePPL(
        "source=people | eval `ROUND(12.34)` = ROUND(12.34), `ROUND(12.34, 1)` = ROUND(12.34, 1),"
            + " `ROUND(12.34, -1)` = ROUND(12.34, -1), `ROUND(12, 1)` = ROUND(12, 1) | fields"
            + " `ROUND(12.34)`, `ROUND(12.34, 1)`, `ROUND(12.34, -1)`, `ROUND(12, 1)`",
        List.of(
            Math.round(12.34),
            Math.round(12.34 * 10) / 10.0,
            Math.round(12.34 / 10) * 10.0,
            Math.round(12.0 * 10) / 10.0));
  }

  @Test
  public void testSign() {
    testSimplePPL(
        "source=people | eval `SIGN(1)` = SIGN(1), `SIGN(0)` = SIGN(0), `SIGN(-1.1)` = SIGN(-1.1) |"
            + " fields `SIGN(1)`, `SIGN(0)`, `SIGN(-1.1)`",
        List.of(1, 0, -1));
  }

  @Test
  public void testSin() {
    testSimplePPL(
        "source=people | eval `SIN(0)` = SIN(0) | fields `SIN(0)`", List.of(Math.sin(0.0)));
  }

  @Test
  public void testSqrt() {
    testSimplePPL(
        "source=people | eval `SQRT(4)` = SQRT(4), `SQRT(4.41)` = SQRT(4.41) | fields `SQRT(4)`,"
            + " `SQRT(4.41)`",
        List.of(Math.sqrt(4), Math.sqrt(4.41)));
  }

  @Test
  public void testCbrt() {
    testSimplePPL(
        "source=people | eval `CBRT(8)` = CBRT(8), `CBRT(9.261)` = CBRT(9.261), `CBRT(-27)` ="
            + " CBRT(-27) | fields `CBRT(8)`, `CBRT(9.261)`, `CBRT(-27)`",
        List.of(Math.cbrt(8), Math.cbrt(9.261), Math.cbrt(-27)));
  }
}
