/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CalcitePPLIPFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.GEOIP);
    loadIndex(Index.WEBLOG);
  }

  @Test
  public void testCidrMatch() {
    // No matches
    JSONObject resultNoMatch =
        executeQuery(
            String.format(
                "source=%s | where cidrmatch(host, '250.0.0.0/24') | fields host",
                TEST_INDEX_WEBLOGS));
    verifySchema(resultNoMatch, schema("host", null, "ip"));
    verifyDataRows(resultNoMatch);

    // One match
    JSONObject resultSingleMatch =
        executeQuery(
            String.format(
                "source=%s | where cidrmatch(host, '0.0.0.0/24') | fields host",
                TEST_INDEX_WEBLOGS));
    verifySchema(resultSingleMatch, schema("host", null, "ip"));
    verifyDataRows(resultSingleMatch, rows("0.0.0.2"));

    // Multiple matches
    JSONObject resultMultipleMatch =
        executeQuery(
            String.format(
                "source=%s | where cidrmatch(host, '1.2.3.0/24') | fields host",
                TEST_INDEX_WEBLOGS));
    verifySchema(resultMultipleMatch, schema("host", null, "ip"));
    verifyDataRows(resultMultipleMatch, rows("1.2.3.4"), rows("1.2.3.5"));

    JSONObject resultIpv6 =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval m4 = CIDRMATCH('192.169.1.5', '192.169.1.0/24'), m6 ="
                    + " CIDRMATCH('2003:0db8:0000:0000:0000:0000:0000:0000', '2003:db8::/32') |"
                    + " fields m4, m6",
                TEST_INDEX_WEBLOGS));
    verifySchema(resultIpv6, schema("m4", "boolean"), schema("m6", "boolean"));
    verifyDataRows(resultIpv6, rows(true, true));
  }
}
