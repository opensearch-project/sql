/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class CommentIT extends PPLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testMultipleLinesCommand() throws IOException {
    // source=accounts
    // | fields firstname
    // | where firstname='Amber'
    // | fields firstname
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s \\n"
                    + "| fields firstname \\n"
                    + "| where firstname='Amber' \\n"
                    + "| fields firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testLineComment() throws IOException {
    // // line comment
    // source=accounts | fields firstname // line comment
    // | where firstname='Amber' // line comment
    // | fields firstname // line comment
    // /////////line comment
    JSONObject result =
        executeQuery(
            String.format(
                "// line comment\\n"
                    + "source=%s | fields firstname // line comment\\n"
                    + "| where firstname='Amber' // line comment\\n"
                    + "| fields firstname // line comment\\n"
                    + "/////////line comment",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }

  @Test
  public void testBlockComment() throws IOException {
    // /*
    // This is a
    //     multiple
    // line block
    //     comment */
    // search /* block comment */ source=accounts /* block comment */
    // | fields /*
    //     This is a
    //         multiple
    //     line
    //         block
    //             comment */ firstname
    // | /* block comment */ where firstname='Amber'
    // | fields /* block comment */ firstname
    JSONObject result =
        executeQuery(
            String.format(
                "/*\\n"
                    + "This is a\\n"
                    + "    multiple\\n"
                    + "line block\\n"
                    + "    comment */\\n"
                    + "search /* block comment */ source=%s /* block comment */\\n"
                    + "| fields /*\\n"
                    + "    This is a\\n"
                    + "        multiple\\n"
                    + "    line\\n"
                    + "        block\\n"
                    + "            comment */ firstname\\n"
                    + "| /* block comment */ where firstname='Amber'\\n"
                    + "| fields /* block comment */ firstname",
                TEST_INDEX_ACCOUNT));
    verifyDataRows(result, rows("Amber"));
  }
}
