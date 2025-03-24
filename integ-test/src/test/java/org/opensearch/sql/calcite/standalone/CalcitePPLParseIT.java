/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class CalcitePPLParseIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testParseEmail() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | parse email '.+@(?<host>.+)' | fields email, host
                   """,
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
  public void testParseOverriding() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | parse email '.+@(?<email>.+)' | fields email
                   """,
                TEST_INDEX_BANK));
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
  public void testParseEmailCountByHost() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | parse email '.+@(?<host>.+)' | stats count() by host
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("count()", "long"), schema("host", "string"));
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
  public void testParseStreetNumber() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | parse address '(?<streetNumber>\\d+)'
                   | eval streetNumberInt = cast(streetNumber as integer)
                   | where streetNumberInt > 500
                   | sort streetNumberInt
                   | fields streetNumberInt, address
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("streetNumberInt", "integer"), schema("address", "string"));
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
    RuntimeException e =
        assertThrows(
            RuntimeException.class,
            () ->
                executeQuery(
                    String.format(
                        """
                    source = %s | parse address '(?<streetNumber>\\d+) (?<street>.+)'
                    | fields streetNumber, street
                    """,
                        TEST_INDEX_BANK)));
    verifyErrorMessageContains(
        e, "Multiple capturing groups (count=2) not allowed in regex input for REGEXP_EXTRACT");
  }
}
