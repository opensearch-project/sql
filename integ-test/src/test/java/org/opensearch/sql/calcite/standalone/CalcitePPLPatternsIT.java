/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

public class CalcitePPLPatternsIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.BANK);
  }

  @Test
  public void testSimplePattern() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns email | fields email, patterns_field
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("patterns_field", "string"));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "@."),
        rows("hattiebond@netagy.com", "@."),
        rows("nanettebates@quility.com", "@."),
        rows("daleadams@boink.com", "@."),
        rows("elinorratliff@scentric.com", "@."),
        rows("virginiaayala@filodyne.com", "@."),
        rows("dillardmcpherson@quailcom.com", "@."));
  }

  @Test
  public void testSimplePatternGroupByPatternsField() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns email | stats count() by patterns_field
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("count()", "long"), schema("patterns_field", "string"));
    verifyDataRows(result, rows(7, "@."));
  }

  @Test
  public void testSimplePatternWithCustomPattern() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns pattern='@.*' email | fields email, patterns_field
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("patterns_field", "string"));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "amberduke"),
        rows("hattiebond@netagy.com", "hattiebond"),
        rows("nanettebates@quility.com", "nanettebates"),
        rows("daleadams@boink.com", "daleadams"),
        rows("elinorratliff@scentric.com", "elinorratliff"),
        rows("virginiaayala@filodyne.com", "virginiaayala"),
        rows("dillardmcpherson@quailcom.com", "dillardmcpherson"));
  }

  @Test
  public void testSimplePatternWithCustomNewField() {
    JSONObject result =
        executeQuery(
            String.format(
                """
                   source = %s | patterns pattern='@.*' new_field='username' email | fields email, username
                   """,
                TEST_INDEX_BANK));
    verifySchema(result, schema("email", "string"), schema("username", "string"));
    verifyDataRows(
        result,
        rows("amberduke@pyrami.com", "amberduke"),
        rows("hattiebond@netagy.com", "hattiebond"),
        rows("nanettebates@quility.com", "nanettebates"),
        rows("daleadams@boink.com", "daleadams"),
        rows("elinorratliff@scentric.com", "elinorratliff"),
        rows("virginiaayala@filodyne.com", "virginiaayala"),
        rows("dillardmcpherson@quailcom.com", "dillardmcpherson"));
  }
}
