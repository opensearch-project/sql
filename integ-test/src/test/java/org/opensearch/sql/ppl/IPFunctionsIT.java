/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class IPFunctionsIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.WEBLOGS);
  }

  @Test
  public void test_cidrmatch() throws IOException {

    JSONObject result;

    // No matches
    result = executeQuery(Index.WEBLOGS.ppl("where cidrmatch(host, '250.0.0.0/24') | fields host"));
    verifySchema(result, schema("host", null, "ip"));
    verifyDataRows(result);

    // One match
    result = executeQuery(Index.WEBLOGS.ppl("where cidrmatch(host, '0.0.0.0/24') | fields host"));
    verifySchema(result, schema("host", null, "ip"));
    verifyDataRows(result, rows("0.0.0.2"));

    // Multiple matches
    result = executeQuery(Index.WEBLOGS.ppl("where cidrmatch(host, '1.2.3.0/24') | fields host"));
    verifySchema(result, schema("host", null, "ip"));
    verifyDataRows(result, rows("1.2.3.4"), rows("1.2.3.5"));
  }
}
