/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLParseIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testParseEmail() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | parse email '.+@(?<host>.+)' | fields email, host",
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("host", "string"));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "pyrami.com"),
        rows("hattiebond@netagy.com", "netagy.com"),
        rows("nanettebates@quility.com", "quility.com"),
        rows("daleadams@boink.com", "boink.com"),
        rows("elinorratliff@scentric.com", "scentric.com"),
        rows("virginiaayala@filodyne.com", "filodyne.com"),
        rows("dillardmcpherson@quailcom.com", "quailcom.com"));
  }

  @Test
  public void testParseOverriding() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | parse email '.+@(?<email>.+)' | fields email", TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"));
    verifyDataRows(
        result,
        rows("pyrami.com"),
        rows("netagy.com"),
        rows("quility.com"),
        rows("boink.com"),
        rows("scentric.com"),
        rows("filodyne.com"),
        rows("quailcom.com"));
  }

  @Test
  public void testParseEmailCountByHost() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | parse email '.+@(?<host>.+)' | stats count() by host",
                TEST_INDEX_BANK));
    verifySchema(result, schema("count()", "bigint"), schema("host", "string"));
    verifyDataRows(
        result,
        rows(1, "pyrami.com"),
        rows(1, "netagy.com"),
        rows(1, "quility.com"),
        rows(1, "boink.com"),
        rows(1, "scentric.com"),
        rows(1, "filodyne.com"),
        rows(1, "quailcom.com"));
  }

  @Test
  public void testParseStreetNumber() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source = %s | parse address '(?<streetNumber>\\\\d+)'"
                    + "| eval streetNumberInt = cast(streetNumber as integer)"
                    + "| where streetNumberInt > 500"
                    + "| sort streetNumberInt"
                    + "| fields streetNumberInt, address",
                TEST_INDEX_BANK));
    verifySchema(result, schema("streetNumberInt", "int"), schema("address", "string"));
    verifyDataRows(
        result,
        rows(671, "671 Bristol Street"),
        rows(702, "702 Quentin Street"),
        rows(789, "789 Madison Street"),
        rows(880, "880 Holmes Lane"));
  }

  // TODO Multiple capturing groups are not allowed in Calcite REGEXP_EXTRACT function.
  // https://github.com/opensearch-project/sql/issues/3472
  @Test
  public void testParseMultipleGroups() {
    Throwable e =
        assertThrowsWithReplace(
            RuntimeException.class,
            () ->
                executeQuery(
                    String.format(
                        "source = %s | parse address '(?<streetNumber>\\\\d+) (?<street>.+)'"
                            + "| fields streetNumber, street",
                        TEST_INDEX_BANK)));
    verifyErrorMessageContains(
        e, "Multiple capturing groups (count=2) not allowed in regex input for REGEXP_EXTRACT");
  }

  @Test
  public void testParseOverriding2() throws IOException {
    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity(
        "{\"email\": \"a@a.com\", \"email0\": \"b@b.com\", \"email1\": \"c@c.com\"}");
    client().performRequest(request1);
    JSONObject result;
    result =
        executeQuery(
            "source = test | parse email '.+@(?<email0>.+)' | fields email, email0, email1");
    verifyDataRows(result, rows("a@a.com", "a.com", "c@c.com"));
    result =
        executeQuery(
            "source = test | parse email '.+@(?<email>.+)' | fields email, email0, email1");
    verifyDataRows(result, rows("a.com", "b@b.com", "c@c.com"));
  }
}
