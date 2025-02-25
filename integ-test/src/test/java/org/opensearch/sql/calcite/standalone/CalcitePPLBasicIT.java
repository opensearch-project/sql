/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;

import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalcitePPLBasicIT extends CalcitePPLTestCase {

  @Override
  public void init() throws IOException {
    super.init();
    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);

    loadIndex(Index.BANK);
  }

  @Test
  public void testInvalidTable() {
    assertThrows(
        "OpenSearch exception [type=index_not_found_exception, reason=no such index [unknown]]",
        IllegalStateException.class,
        () -> execute("source=unknown"));
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
}
