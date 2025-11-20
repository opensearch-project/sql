/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.LikeQueryIT;

public class CalciteLikeQueryIT extends LikeQueryIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Override
  @Test
  public void test_convert_field_text_to_keyword() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    super.test_convert_field_text_to_keyword();
  }

  @Test
  public void test_like_should_be_case_sensitive() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, 'test Wildcard%') | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyNumOfRows(result, 0);
  }

  @Test
  public void test_ilike_is_case_insensitive() throws IOException {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE ILike(KeywordBody, 'test Wildcard%') | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(
        result,
        rows("test wildcard"),
        rows("test wildcard in the end of the text%"),
        rows("test wildcard in % the middle of the text"),
        rows("test wildcard %% beside each other"),
        rows("test wildcard in the end of the text_"),
        rows("test wildcard in _ the middle of the text"),
        rows("test wildcard __ beside each other"));
  }
}
