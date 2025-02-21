/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** testSortXXAndXX could fail. TODO Remove this @Ignore when the issue fixed. */
// @Ignore
public class CalcitePPLSortIT extends CalcitePPLTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.BANK);
  }

  @Test
  public void testFieldsAndSort1() {
    String actual =
        execute(
            String.format(
                "source=%s | fields + firstname, gender, account_number | sort - account_number",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"account_number\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Dillard\",\n"
            + "      \"F\",\n"
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Virginia\",\n"
            + "      \"F\",\n"
            + "      25\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Elinor\",\n"
            + "      \"M\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Dale\",\n"
            + "      \"M\",\n"
            + "      18\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Nanette\",\n"
            + "      \"F\",\n"
            + "      13\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hattie\",\n"
            + "      \"M\",\n"
            + "      6\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Amber JOHnny\",\n"
            + "      \"M\",\n"
            + "      1\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}",
        actual);
  }

  @Test
  public void testFieldsAndSort2() {
    String actual = execute(String.format("source=%s | fields age | sort - age", TEST_INDEX_BANK));
    String expected =
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      39\n"
            + "    ],\n"
            + "    [\n"
            + "      36\n"
            + "    ],\n"
            + "    [\n"
            + "      36\n"
            + "    ],\n"
            + "    [\n"
            + "      34\n"
            + "    ],\n"
            + "    [\n"
            + "      33\n"
            + "    ],\n"
            + "    [\n"
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      28\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}";
    assertEquals(expected, actual);
  }

  @Test
  public void testFieldsAndSortTwoFields() {
    String actual =
        execute(
            String.format(
                "source=%s | fields + firstname, gender, account_number | sort + gender, -"
                    + " account_number",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"account_number\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Dillard\",\n"
            + "      \"F\",\n"
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Virginia\",\n"
            + "      \"F\",\n"
            + "      25\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Nanette\",\n"
            + "      \"F\",\n"
            + "      13\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Elinor\",\n"
            + "      \"M\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Dale\",\n"
            + "      \"M\",\n"
            + "      18\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hattie\",\n"
            + "      \"M\",\n"
            + "      6\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Amber JOHnny\",\n"
            + "      \"M\",\n"
            + "      1\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}",
        actual);
  }

  @Test
  public void testFieldsAndSortWithDescAndLimit() {
    String actual =
        execute(
            String.format(
                "source=%s | fields + firstname, gender, account_number | sort + gender, -"
                    + " account_number | head 5",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"gender\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"account_number\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Dillard\",\n"
            + "      \"F\",\n"
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Virginia\",\n"
            + "      \"F\",\n"
            + "      25\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Nanette\",\n"
            + "      \"F\",\n"
            + "      13\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Elinor\",\n"
            + "      \"M\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Dale\",\n"
            + "      \"M\",\n"
            + "      18\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 5,\n"
            + "  \"size\": 5\n"
            + "}",
        actual);
  }

  @Test
  public void testSortAccountAndFieldsAccount() {
    String actual =
        execute(
            String.format(
                "source=%s | sort - account_number | fields account_number", TEST_INDEX_BANK));
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
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      25\n"
            + "    ],\n"
            + "    [\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      18\n"
            + "    ],\n"
            + "    [\n"
            + "      13\n"
            + "    ],\n"
            + "    [\n"
            + "      6\n"
            + "    ],\n"
            + "    [\n"
            + "      1\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}",
        actual);
  }

  @Test
  public void testSortAccountAndFieldsNameAccount() {
    String actual =
        execute(
            String.format(
                "source=%s | sort - account_number | fields firstname, account_number",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"account_number\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Dillard\",\n"
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Virginia\",\n"
            + "      25\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Elinor\",\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Dale\",\n"
            + "      18\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Nanette\",\n"
            + "      13\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hattie\",\n"
            + "      6\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Amber JOHnny\",\n"
            + "      1\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}",
        actual);
  }

  @Test
  public void testSortAccountAndFieldsAccountName() {
    String actual =
        execute(
            String.format(
                "source=%s | sort - account_number | fields account_number, firstname",
                TEST_INDEX_BANK));
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"account_number\",\n"
            + "      \"type\": \"long\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      32,\n"
            + "      \"Dillard\"\n"
            + "    ],\n"
            + "    [\n"
            + "      25,\n"
            + "      \"Virginia\"\n"
            + "    ],\n"
            + "    [\n"
            + "      20,\n"
            + "      \"Elinor\"\n"
            + "    ],\n"
            + "    [\n"
            + "      18,\n"
            + "      \"Dale\"\n"
            + "    ],\n"
            + "    [\n"
            + "      13,\n"
            + "      \"Nanette\"\n"
            + "    ],\n"
            + "    [\n"
            + "      6,\n"
            + "      \"Hattie\"\n"
            + "    ],\n"
            + "    [\n"
            + "      1,\n"
            + "      \"Amber JOHnny\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}",
        actual);
  }

  @Test
  public void testSortAgeAndFieldsAge() {
    String actual = execute(String.format("source=%s | sort - age | fields age", TEST_INDEX_BANK));
    String expected =
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      39\n"
            + "    ],\n"
            + "    [\n"
            + "      36\n"
            + "    ],\n"
            + "    [\n"
            + "      36\n"
            + "    ],\n"
            + "    [\n"
            + "      34\n"
            + "    ],\n"
            + "    [\n"
            + "      33\n"
            + "    ],\n"
            + "    [\n"
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      28\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}";
    assertEquals(expected, actual);
  }

  @Test
  public void testSortAgeAndFieldsNameAge() {
    String actual =
        execute(String.format("source=%s | sort - age | fields firstname, age", TEST_INDEX_BANK));
    String expected =
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Virginia\",\n"
            + "      39\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hattie\",\n"
            + "      36\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Elinor\",\n"
            + "      36\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Dillard\",\n"
            + "      34\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Dale\",\n"
            + "      33\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Amber JOHnny\",\n"
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Nanette\",\n"
            + "      28\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}";
    assertEquals(expected, actual);
  }

  @Test
  public void testSortAgeNameAndFieldsNameAge() {
    String actual =
        execute(
            String.format(
                "source=%s | sort - age, - firstname | fields firstname, age", TEST_INDEX_BANK));
    String expected =
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"firstname\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Virginia\",\n"
            + "      39\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hattie\",\n"
            + "      36\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Elinor\",\n"
            + "      36\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Dillard\",\n"
            + "      34\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Dale\",\n"
            + "      33\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Amber JOHnny\",\n"
            + "      32\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Nanette\",\n"
            + "      28\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 7,\n"
            + "  \"size\": 7\n"
            + "}";
    assertEquals(expected, actual);
  }
}
