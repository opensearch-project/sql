/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.correctness.tests;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.fail;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.correctness.report.ErrorTestCase;
import org.opensearch.sql.correctness.report.FailedTestCase;
import org.opensearch.sql.correctness.report.SuccessTestCase;
import org.opensearch.sql.correctness.report.TestReport;
import org.opensearch.sql.correctness.runner.resultset.DBResult;
import org.opensearch.sql.correctness.runner.resultset.Row;
import org.opensearch.sql.correctness.runner.resultset.Type;

/** Test for {@link TestReport} */
public class TestReportTest {

  private final TestReport report = new TestReport();

  @Test
  public void testSuccessReport() {
    report.addTestCase(new SuccessTestCase(1, "SELECT * FROM accounts"));
    JSONObject actual = new JSONObject(report);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"summary\": {"
                + "    \"total\": 1,"
                + "    \"success\": 1,"
                + "    \"failure\": 0"
                + "  },"
                + "  \"tests\": ["
                + "    {"
                + "      \"id\": 1,"
                + "      \"result\": 'Success',"
                + "      \"sql\": \"SELECT * FROM accounts\","
                + "    }"
                + "  ]"
                + "}");

    if (!actual.similar(expected)) {
      fail("Actual JSON is different from expected: " + actual.toString(2));
    }
  }

  @Test
  public void testFailedReport() {
    report.addTestCase(
        new FailedTestCase(
            1,
            "SELECT * FROM accounts",
            asList(
                new DBResult(
                    "OpenSearch",
                    singleton(new Type("firstName", "text")),
                    singleton(new Row(asList("hello")))),
                new DBResult(
                    "H2",
                    singleton(new Type("firstName", "text")),
                    singleton(new Row(asList("world"))))),
            "[SQLITE_ERROR] SQL error or missing database;"));
    JSONObject actual = new JSONObject(report);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"summary\": {"
                + "    \"total\": 1,"
                + "    \"success\": 0,"
                + "    \"failure\": 1"
                + "  },"
                + "  \"tests\": ["
                + "    {"
                + "      \"id\": 1,"
                + "      \"result\": 'Failed',"
                + "      \"sql\": \"SELECT * FROM accounts\","
                + "      \"explain\": \"Data row at [0] is different: "
                + "this=[Row(values=[world])], "
                + "other=[Row(values=[hello])]\","
                + "      \"errors\": \"[SQLITE_ERROR] SQL error or missing database;\","
                + "      \"resultSets\": ["
                + "        {"
                + "          \"database\": \"H2\","
                + "          \"schema\": ["
                + "            {"
                + "              \"name\": \"firstName\","
                + "              \"type\": \"text\""
                + "            }"
                + "          ],"
                + "          \"dataRows\": [[\"world\"]]"
                + "        },"
                + "        {"
                + "          \"database\": \"OpenSearch\","
                + "          \"schema\": ["
                + "            {"
                + "              \"name\": \"firstName\","
                + "              \"type\": \"text\""
                + "            }"
                + "          ],"
                + "          \"dataRows\": [[\"hello\"]]"
                + "        }"
                + "      ]"
                + "    }"
                + "  ]"
                + "}");

    if (!actual.similar(expected)) {
      fail("Actual JSON is different from expected: " + actual.toString(2));
    }
  }

  @Test
  public void testErrorReport() {
    report.addTestCase(new ErrorTestCase(1, "SELECT * FROM", "Missing table name in query"));
    JSONObject actual = new JSONObject(report);
    JSONObject expected =
        new JSONObject(
            "{"
                + "  \"summary\": {"
                + "    \"total\": 1,"
                + "    \"success\": 0,"
                + "    \"failure\": 1"
                + "  },"
                + "  \"tests\": ["
                + "    {"
                + "      \"id\": 1,"
                + "      \"result\": 'Failed',"
                + "      \"sql\": \"SELECT * FROM\","
                + "      \"reason\": \"Missing table name in query\","
                + "    }"
                + "  ]"
                + "}");

    if (!actual.similar(expected)) {
      fail("Actual JSON is different from expected: " + actual.toString(2));
    }
  }
}
