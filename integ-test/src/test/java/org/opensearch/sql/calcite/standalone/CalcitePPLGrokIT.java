/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;

public class CalcitePPLGrokIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.BANK);
    loadIndex(Index.WEBLOG);
  }

  @Test
  public void testGrokEmail() {
    JSONObject result =
        executeQuery(
            String.format(
                Locale.ROOT,
                """
                   source = %s | grok email '%s' | head 3 | fields email, host
                   """,
                TEST_INDEX_BANK, ".+@%{HOSTNAME:host}"));
    verifySchema(result, schema("email", "string"), schema("host", "string"));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "pyrami.com"),
        rows("hattiebond@netagy.com", "netagy.com"),
        rows("nanettebates@quility.com", "quility.com"));
  }

  @Test
  public void testGrokAddressOverriding() {
    JSONObject preGrokResult =
        executeQuery(
            String.format(
                Locale.ROOT,
                """
                   source = %s | head 3 | fields address
                   """,
                TEST_INDEX_BANK));
    verifySchema(preGrokResult, schema("address", "string"));
    verifyDataRows(
        preGrokResult,
        rows("880 Holmes Lane"),
        rows("671 Bristol Street"),
        rows("789 Madison Street"));

    JSONObject result =
        executeQuery(
            String.format(
                Locale.ROOT,
                """
                   source = %s | grok address '%s' | head 3 | fields address
                   """,
                TEST_INDEX_BANK, "%{NUMBER} %{GREEDYDATA:address}"));
    verifySchema(result, schema("address", "string"));
    verifyDataRows(result, rows("Holmes Lane"), rows("Bristol Street"), rows("Madison Street"));
  }

  @Test
  public void testGrokApacheLog() {
    JSONObject result =
        executeQuery(
            String.format(
                Locale.ROOT,
                """
                   source = %s | grok message '%s' | fields message, timestamp, response
                   """,
                TEST_INDEX_WEBLOGS, "%{COMMONAPACHELOG}"));
    verifySchema(
        result,
        schema("message", "string"),
        schema("timestamp", "string"),
        schema("response", "string"));
    verifyDataRows(
        result,
        rows(
            "177.95.8.74 - upton5450 [28/Sep/2022:10:15:57 -0700] \"HEAD /e-business/mindshare"
                + " HTTP/1.0\" 404 19927",
            "28/Sep/2022:10:15:57 -0700",
            "404"),
        rows(
            "127.45.152.6 - pouros8756 [28/Sep/2022:10:15:57 -0700] \"GET"
                + " /architectures/convergence/niches/mindshare HTTP/1.0\" 100 28722",
            "28/Sep/2022:10:15:57 -0700",
            "100"),
        rows(null, null, null),
        rows(
            "118.223.210.105 - - [28/Sep/2022:10:15:57 -0700] \"PATCH /strategize/out-of-the-box"
                + " HTTP/1.0\" 401 27439",
            "28/Sep/2022:10:15:57 -0700",
            "401"),
        rows(
            "210.204.15.104 - - [28/Sep/2022:10:15:57 -0700] \"POST /users HTTP/1.1\" 301 9481",
            "28/Sep/2022:10:15:57 -0700",
            "301"),
        rows(null, null, null));
  }
}
