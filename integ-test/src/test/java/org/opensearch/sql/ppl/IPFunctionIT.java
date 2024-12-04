/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOG;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class IPFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.WEBLOG);
  }

  @Test
  public void test_cidrmatch() throws IOException {

    // TODO #3145: Add tests for IP address data type.
    JSONObject result;

    // No matches
    result =
        executeQuery(
            String.format(
                "source=%s | where cidrmatch(host_string, '199.120.111.0/24') | fields host_string",
                TEST_INDEX_WEBLOG));
    verifySchema(result, schema("host_string", null, "string"));
    verifyDataRows(result);

    // One match
    result =
        executeQuery(
            String.format(
                "source=%s | where cidrmatch(host_string, '199.120.110.0/24') | fields host_string",
                TEST_INDEX_WEBLOG));
    verifySchema(result, schema("host_string", null, "string"));
    verifyDataRows(result, rows("199.120.110.21"));

    // Multiple matches
    result =
        executeQuery(
            String.format(
                "source=%s | where cidrmatch(host_string, '199.0.0.0/8') | fields host_string",
                TEST_INDEX_WEBLOG));
    verifySchema(result, schema("host_string", null, "string"));
    verifyDataRows(result, rows("199.72.81.55"), rows("199.120.110.21"));
  }
}
