/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class ParseCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testParseCommand() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | parse email '.+@(?<host>.+)' | fields email, host", TEST_INDEX_BANK));
    verifyOrder(
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
  public void testParseCommandReplaceOriginalField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | parse email '.+@(?<email>.+)' | fields email", TEST_INDEX_BANK));
    verifyOrder(
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
  public void testParseCommandWithOtherRunTimeFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | parse email '.+@(?<host>.+)' | "
                    + "eval eval_result=1 | fields host, eval_result",
                TEST_INDEX_BANK));
    verifyOrder(
        result,
        rows("pyrami.com", 1),
        rows("netagy.com", 1),
        rows("quility.com", 1),
        rows("boink.com", 1),
        rows("scentric.com", 1),
        rows("filodyne.com", 1),
        rows("quailcom.com", 1));
  }
}
