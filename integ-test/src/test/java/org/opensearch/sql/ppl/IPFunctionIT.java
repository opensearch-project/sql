/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOG;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class IPFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.WEBLOG);
  }

  @Test
  public void testCidr() throws IOException {

    JSONObject result;

    // No matches
    result =
        executeQuery(
            String.format(
                "source=%s | where cidr(host, '199.120.111.0/24') | fields url",
                TEST_INDEX_WEBLOG));
    verifySchema(result, schema("url", null, "boolean"));
    verifyDataRows(result, rows("/shuttle/missions/sts-73/mission-sts-73.html"));

    // One match
    result =
        executeQuery(
            String.format(
                "source=%s | where cidr(host, '199.120.110.0/24') | fields url",
                TEST_INDEX_WEBLOG));
    verifySchema(result, schema("url", null, "boolean"));
    verifyDataRows(result, rows("/shuttle/missions/sts-73/mission-sts-73.html"));

    // Multiple matches
    result =
        executeQuery(
            String.format(
                "source=%s | where cidr(host, '199.0.0.0/8') | fields url", TEST_INDEX_WEBLOG));
    verifySchema(result, schema("url", null, "boolean"));
    verifyDataRows(
        result, rows("/history/apollo/"), rows("/shuttle/missions/sts-73/mission-sts-73.html"));
  }
}
