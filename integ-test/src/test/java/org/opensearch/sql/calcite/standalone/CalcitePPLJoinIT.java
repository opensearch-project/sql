/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_HOBBIES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OCCUPATION;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;

import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;

public class CalcitePPLJoinIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.OCCUPATION);
    loadIndex(Index.HOBBIES);
  }

  @Test
  public void testJoinWithCondition() {
    String actual =
        execute(
            String.format(
                "source=%s | inner join left=a, right=b ON a.name = b.name AND a.year = 2023"
                    + " AND a.month = 4 AND b.year = 2023 AND b.month = 4 %s | fields a.name,"
                    + " a.age, a.state, a.country, b.occupation, b.country, b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"occupation\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"salary\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Jake\",\n"
            + "      70,\n"
            + "      \"California\",\n"
            + "      \"USA\",\n"
            + "      \"Engineer\",\n"
            + "      \"England\",\n"
            + "      100000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hello\",\n"
            + "      30,\n"
            + "      \"New York\",\n"
            + "      \"USA\",\n"
            + "      \"Artist\",\n"
            + "      \"USA\",\n"
            + "      70000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      25,\n"
            + "      \"Ontario\",\n"
            + "      \"Canada\",\n"
            + "      \"Doctor\",\n"
            + "      \"Canada\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jane\",\n"
            + "      20,\n"
            + "      \"Quebec\",\n"
            + "      \"Canada\",\n"
            + "      \"Scientist\",\n"
            + "      \"Canada\",\n"
            + "      90000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      40,\n"
            + "      \"Washington\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"USA\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      40,\n"
            + "      \"Washington\",\n"
            + "      \"USA\",\n"
            + "      \"Unemployed\",\n"
            + "      \"Canada\",\n"
            + "      0\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 6,\n"
            + "  \"size\": 6\n"
            + "}",
        actual);
  }

  @Test
  public void testJoinWithTwoJoinConditions() {
    String actual =
        execute(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name AND a.country ="
                    + " b.country AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month ="
                    + " 4 %s | fields a.name, a.age, a.state, a.country, b.occupation, b.country,"
                    + " b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"occupation\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"salary\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Hello\",\n"
            + "      30,\n"
            + "      \"New York\",\n"
            + "      \"USA\",\n"
            + "      \"Artist\",\n"
            + "      \"USA\",\n"
            + "      70000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      25,\n"
            + "      \"Ontario\",\n"
            + "      \"Canada\",\n"
            + "      \"Doctor\",\n"
            + "      \"Canada\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jane\",\n"
            + "      20,\n"
            + "      \"Quebec\",\n"
            + "      \"Canada\",\n"
            + "      \"Scientist\",\n"
            + "      \"Canada\",\n"
            + "      90000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      40,\n"
            + "      \"Washington\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"USA\",\n"
            + "      120000\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 4,\n"
            + "  \"size\": 4\n"
            + "}",
        actual);
  }

  @Test
  public void testJoinTwoColumnsAndDisjointFilters() {
    String actual =
        execute(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name AND a.country ="
                    + " b.country AND a.year = 2023 AND a.month = 4 AND b.salary > 100000 %s |"
                    + " fields a.name, a.age, a.state, a.country, b.occupation, b.country,"
                    + " b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"occupation\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"salary\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      25,\n"
            + "      \"Ontario\",\n"
            + "      \"Canada\",\n"
            + "      \"Doctor\",\n"
            + "      \"Canada\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      40,\n"
            + "      \"Washington\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"USA\",\n"
            + "      120000\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Test
  public void testJoinThenStats() {
    String actual =
        execute(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name %s | stats avg(salary)"
                    + " by span(age, 10) as age_span",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"age_span\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"avg(salary)\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      70.0,\n"
            + "      100000.0\n"
            + "    ],\n"
            + "    [\n"
            + "      40.0,\n"
            + "      60000.0\n"
            + "    ],\n"
            + "    [\n"
            + "      20.0,\n"
            + "      105000.0\n"
            + "    ],\n"
            + "    [\n"
            + "      30.0,\n"
            + "      70000.0\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 4,\n"
            + "  \"size\": 4\n"
            + "}",
        actual);
  }

  @Test
  public void testJoinThenStatsWithGroupBy() {
    String actual =
        execute(
            String.format(
                "source = %s | inner join left=a, right=b ON a.name = b.name %s | stats avg(salary)"
                    + " by span(age, 10) as age_span, b.country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"b.country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age_span\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"avg(salary)\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Canada\",\n"
            + "      40.0,\n"
            + "      0.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Canada\",\n"
            + "      20.0,\n"
            + "      105000.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"USA\",\n"
            + "      40.0,\n"
            + "      120000.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"England\",\n"
            + "      70.0,\n"
            + "      100000.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"USA\",\n"
            + "      30.0,\n"
            + "      70000.0\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 5,\n"
            + "  \"size\": 5\n"
            + "}",
        actual);
  }

  @Test
  public void testComplexInnerJoin() {
    String actual =
        execute(
            String.format(
                "source = %s | where country = 'USA' OR country = 'England' | inner join left=a,"
                    + " right=b ON a.name = b.name %s | stats avg(salary) by span(age, 10) as"
                    + " age_span, b.country",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"b.country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age_span\",\n"
            + "      \"type\": \"double\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"avg(salary)\",\n"
            + "      \"type\": \"double\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Canada\",\n"
            + "      40.0,\n"
            + "      0.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"USA\",\n"
            + "      40.0,\n"
            + "      120000.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"England\",\n"
            + "      70.0,\n"
            + "      100000.0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"USA\",\n"
            + "      30.0,\n"
            + "      70000.0\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 4,\n"
            + "  \"size\": 4\n"
            + "}",
        actual);
  }

  @Test
  public void testComplexLeftJoin() {
    String actual =
        execute(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | left join left=a,"
                    + " right=b ON a.name = b.name %s | sort a.age | fields a.name, a.age, a.state,"
                    + " a.country, b.occupation, b.country, b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"occupation\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"salary\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Jane\",\n"
            + "      20,\n"
            + "      \"Quebec\",\n"
            + "      \"Canada\",\n"
            + "      \"Scientist\",\n"
            + "      \"Canada\",\n"
            + "      90000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      25,\n"
            + "      \"Ontario\",\n"
            + "      \"Canada\",\n"
            + "      \"Doctor\",\n"
            + "      \"Canada\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jim\",\n"
            + "      27,\n"
            + "      \"B.C\",\n"
            + "      \"Canada\",\n"
            + "      null,\n"
            + "      null,\n"
            + "      0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Peter\",\n"
            + "      57,\n"
            + "      \"B.C\",\n"
            + "      \"Canada\",\n"
            + "      null,\n"
            + "      null,\n"
            + "      0\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Rick\",\n"
            + "      70,\n"
            + "      \"B.C\",\n"
            + "      \"Canada\",\n"
            + "      null,\n"
            + "      null,\n"
            + "      0\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 5,\n"
            + "  \"size\": 5\n"
            + "}",
        actual);
  }

  @Test
  public void testComplexRightJoin() {
    String actual =
        execute(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | right join left=a,"
                    + " right=b ON a.name = b.name %s | sort a.age | fields a.name, a.age, a.state,"
                    + " a.country, b.occupation, b.country, b.salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"occupation\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"salary\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Jane\",\n"
            + "      20,\n"
            + "      \"Quebec\",\n"
            + "      \"Canada\",\n"
            + "      \"Scientist\",\n"
            + "      \"Canada\",\n"
            + "      90000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      25,\n"
            + "      \"Ontario\",\n"
            + "      \"Canada\",\n"
            + "      \"Doctor\",\n"
            + "      \"Canada\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      null,\n"
            + "      0,\n"
            + "      null,\n"
            + "      null,\n"
            + "      \"Engineer\",\n"
            + "      \"England\",\n"
            + "      100000\n"
            + "    ],\n"
            + "    [\n"
            + "      null,\n"
            + "      0,\n"
            + "      null,\n"
            + "      null,\n"
            + "      \"Artist\",\n"
            + "      \"USA\",\n"
            + "      70000\n"
            + "    ],\n"
            + "    [\n"
            + "      null,\n"
            + "      0,\n"
            + "      null,\n"
            + "      null,\n"
            + "      \"Doctor\",\n"
            + "      \"USA\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      null,\n"
            + "      0,\n"
            + "      null,\n"
            + "      null,\n"
            + "      \"Unemployed\",\n"
            + "      \"Canada\",\n"
            + "      0\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 6,\n"
            + "  \"size\": 6\n"
            + "}",
        actual);
  }

  @Test
  public void testComplexSemiJoin() {
    String actual =
        execute(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | left semi join"
                    + " left=a, right=b ON a.name = b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"month\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"year\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Jane\",\n"
            + "      \"Canada\",\n"
            + "      \"Quebec\",\n"
            + "      4,\n"
            + "      2023,\n"
            + "      20\n"
            + "    ],\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      \"Canada\",\n"
            + "      \"Ontario\",\n"
            + "      4,\n"
            + "      2023,\n"
            + "      25\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 2,\n"
            + "  \"size\": 2\n"
            + "}",
        actual);
  }

  @Test
  public void testComplexAntiJoin() {
    String actual =
        execute(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | left anti join"
                    + " left=a, right=b ON a.name = b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"month\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"year\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Jim\",\n"
            + "      \"Canada\",\n"
            + "      \"B.C\",\n"
            + "      4,\n"
            + "      2023,\n"
            + "      27\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Peter\",\n"
            + "      \"Canada\",\n"
            + "      \"B.C\",\n"
            + "      4,\n"
            + "      2023,\n"
            + "      57\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Rick\",\n"
            + "      \"Canada\",\n"
            + "      \"B.C\",\n"
            + "      4,\n"
            + "      2023,\n"
            + "      70\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 3,\n"
            + "  \"size\": 3\n"
            + "}",
        actual);
  }

  @Test
  public void testComplexCrossJoin() {
    String actual =
        execute(
            String.format(
                "source = %s | where country = 'Canada' OR country = 'England' | join left=a,"
                    + " right=b %s | sort a.age | stats count()",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"count()\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      30\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        actual);
  }

  @Test
  public void testNonEquiJoin() {
    String actual =
        execute(
            String.format(
                "source = %s | where country = 'USA' OR country = 'England' | inner join left=a,"
                    + " right=b ON age < salary %s |  where occupation = 'Doctor' OR occupation ="
                    + " 'Engineer' | fields a.name, age, state, a.country, occupation, b.country,"
                    + " salary",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"age\",\n"
            + "      \"type\": \"integer\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"state\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"occupation\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"country0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"salary\",\n"
            + "      \"type\": \"integer\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"Jake\",\n"
            + "      70,\n"
            + "      \"California\",\n"
            + "      \"USA\",\n"
            + "      \"Engineer\",\n"
            + "      \"England\",\n"
            + "      100000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jake\",\n"
            + "      70,\n"
            + "      \"California\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"Canada\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jake\",\n"
            + "      70,\n"
            + "      \"California\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"USA\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hello\",\n"
            + "      30,\n"
            + "      \"New York\",\n"
            + "      \"USA\",\n"
            + "      \"Engineer\",\n"
            + "      \"England\",\n"
            + "      100000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hello\",\n"
            + "      30,\n"
            + "      \"New York\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"Canada\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hello\",\n"
            + "      30,\n"
            + "      \"New York\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"USA\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      40,\n"
            + "      \"Washington\",\n"
            + "      \"USA\",\n"
            + "      \"Engineer\",\n"
            + "      \"England\",\n"
            + "      100000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      40,\n"
            + "      \"Washington\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"Canada\",\n"
            + "      120000\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      40,\n"
            + "      \"Washington\",\n"
            + "      \"USA\",\n"
            + "      \"Doctor\",\n"
            + "      \"USA\",\n"
            + "      120000\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 9,\n"
            + "  \"size\": 9\n"
            + "}",
        actual);
  }

  @Test
  public void testCrossJoinWithJoinCriteriaFallbackToInnerJoin() {
    String cross =
        execute(
            String.format(
                "source = %s | where country = 'USA' | cross join left=a, right=b ON a.name ="
                    + " b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String inner =
        execute(
            String.format(
                "source = %s | where country = 'USA' | inner join left=a, right=b ON a.name ="
                    + " b.name %s | sort a.age",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(cross, inner);
  }

  @Ignore // TODO seems a calcite bug
  public void testMultipleJoins() {
    String actual =
        execute(
            String.format(
                """
                   source = %s
                   | where country = 'Canada' OR country = 'England'
                   | inner join left=a, right=b
                       ON a.name = b.name AND a.year = 2023 AND a.month = 4 AND b.year = 2023 AND b.month = 4
                       %s
                   | eval a_name = a.name
                   | eval a_country = a.country
                   | eval b_country = b.country
                   | fields a_name, age, state, a_country, occupation, b_country, salary
                   | left join left=a, right=b
                       ON a.a_name = b.name
                       %s
                   | eval aa_country = a.a_country
                   | eval ab_country = a.b_country
                   | eval bb_country = b.country
                   | fields a_name, age, state, aa_country, occupation, ab_country, salary, bb_country, hobby, language
                   | cross join left=a, right=b
                       %s
                   | eval new_country = a.aa_country
                   | eval new_salary = b.salary
                   | stats avg(new_salary) as avg_salary by span(age, 5) as age_span, state
                   | left semi join left=a, right=b
                       ON a.state = b.state
                       %s
                   | eval new_avg_salary = floor(avg_salary)
                   | fields state, age_span, new_avg_salary
                   """,
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_STATE_COUNTRY));
    assertEquals("30", actual);
  }

  @Test
  public void testMultipleJoinsWithoutTableAliases() {
    String actual =
        execute(
            String.format(
                "source = %s | JOIN ON %s.name = %s.name %s | JOIN ON %s.name = %s.name %s | fields"
                    + " %s.name, %s.name, %s.name",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name1\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      \"David\",\n"
            + "      \"David\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      \"David\",\n"
            + "      \"David\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hello\",\n"
            + "      \"Hello\",\n"
            + "      \"Hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jake\",\n"
            + "      \"Jake\",\n"
            + "      \"Jake\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jane\",\n"
            + "      \"Jane\",\n"
            + "      \"Jane\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      \"John\",\n"
            + "      \"John\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 6,\n"
            + "  \"size\": 6\n"
            + "}",
        actual);
  }

  @Test
  public void testMultipleJoinsWithPartTableAliases() {
    String actual =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s | JOIN right = t3"
                    + " ON t1.name = t3.name %s | fields t1.name, t2.name, t3.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION, TEST_INDEX_HOBBIES));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name1\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      \"David\",\n"
            + "      \"David\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      \"David\",\n"
            + "      \"David\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hello\",\n"
            + "      \"Hello\",\n"
            + "      \"Hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jake\",\n"
            + "      \"Jake\",\n"
            + "      \"Jake\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jane\",\n"
            + "      \"Jane\",\n"
            + "      \"Jane\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      \"John\",\n"
            + "      \"John\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 6,\n"
            + "  \"size\": 6\n"
            + "}",
        actual);
  }

  @Test
  public void testMultipleJoinsWithSelfJoin1() {
    String actual =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s | JOIN right = t3"
                    + " ON t1.name = t3.name %s | JOIN right = t4 ON t1.name = t4.name %s | fields"
                    + " t1.name, t2.name, t3.name, t4.name",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_STATE_COUNTRY));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"name\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name0\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name1\",\n"
            + "      \"type\": \"string\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"name2\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      \"David\",\n"
            + "      \"David\",\n"
            + "      \"David\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"David\",\n"
            + "      \"David\",\n"
            + "      \"David\",\n"
            + "      \"David\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Hello\",\n"
            + "      \"Hello\",\n"
            + "      \"Hello\",\n"
            + "      \"Hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jake\",\n"
            + "      \"Jake\",\n"
            + "      \"Jake\",\n"
            + "      \"Jake\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"Jane\",\n"
            + "      \"Jane\",\n"
            + "      \"Jane\",\n"
            + "      \"Jane\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"John\",\n"
            + "      \"John\",\n"
            + "      \"John\",\n"
            + "      \"John\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 6,\n"
            + "  \"size\": 6\n"
            + "}",
        actual);
  }

  @Ignore // TODO subquery not support
  public void testMultipleJoinsWithSelfJoin2() {
    String actual =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s | JOIN right = t3"
                    + " ON t1.name = t3.name %s | JOIN ON t1.name = t4.name [ source = %s ] as t4 |"
                    + " fields t1.name, t2.name, t3.name, t4.name",
                TEST_INDEX_STATE_COUNTRY,
                TEST_INDEX_OCCUPATION,
                TEST_INDEX_HOBBIES,
                TEST_INDEX_STATE_COUNTRY));
    assertEquals("", actual);
  }

  @Test
  public void testCheckAccessTheReferenceByAliases1() {
    String res1 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields t1.name,"
                    + " t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res2 =
        execute(
            String.format(
                "source = %s as t1 | JOIN ON t1.name = t2.name %s as t2 | fields t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res1, res2);

    String res3 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s as tt | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res4 =
        execute(
            String.format(
                "source = %s as tt | JOIN left = t1 right = t2 ON t1.name = t2.name %s | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res5 =
        execute(
            String.format(
                "source = %s as tt | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res3, res4);
    assertEquals(res4, res5);
  }

  @Ignore // TODO subquery not support
  public void testCheckAccessTheReferenceByAliases2() {
    String res1 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s ] as t2 | fields"
                    + " t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res2 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s as t2 ] | fields"
                    + " t1.name, t2.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res1, res2);

    String res3 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s as"
                    + " tt ] | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res4 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 ON t1.name = t2.name [ source = %s as tt ] as t2"
                    + " | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res5 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name [ source = %s ]"
                    + " as tt | fields tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res3, res4);
    assertEquals(res4, res5);
  }

  @Test
  public void testCheckAccessTheReferenceByAliases3() {
    String res1 =
        execute(
            String.format(
                "source = %s | JOIN left = t1 right = t2 ON t1.name = t2.name %s as tt | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res2 =
        execute(
            String.format(
                "source = %s as tt | JOIN left = t1 right = t2 ON t1.name = t2.name %s | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    String res3 =
        execute(
            String.format(
                "source = %s as tt | JOIN left = t1 ON t1.name = t2.name %s as t2 | fields"
                    + " tt.name",
                TEST_INDEX_STATE_COUNTRY, TEST_INDEX_OCCUPATION));
    assertEquals(res1, res2);
    assertEquals(res1, res3);
  }
}
