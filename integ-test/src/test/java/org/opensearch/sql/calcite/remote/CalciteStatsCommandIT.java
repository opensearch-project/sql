/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.StatsCommandIT;

public class CalciteStatsCommandIT extends StatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    setQuerySizeLimit(2000);
  }

  @Test
  public void testPaginatingStatsForHaving() throws IOException {
    try {
      setQueryBucketSize(2);
      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | stats sum(balance) as a by state | where a > 780000",
                  TEST_INDEX_ACCOUNT));
      verifyDataRows(response, rows(782199, "TX"));
    } finally {
      resetQueryBucketSize();
    }
  }

  @Test
  public void testPaginatingStatsForJoin() throws IOException {
    try {
      setQueryBucketSize(2);
      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | stats sum(balance) as a by state | join left=l right=r on l.state ="
                      + " r.state [ source = %s | stats sum(balance) as a by state ]",
                  TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));
      verifyDataRows(
          response,
          rows(648774, "IL", 39225, "IL"),
          rows(346934, "IN", 48086, "IN"),
          rows(732523, "MD", 4180, "MD"),
          rows(531785, "PA", 40540, "PA"),
          rows(709135, "TN", 5686, "TN"),
          rows(489601, "VA", 32838, "VA"),
          rows(483741, "WA", 16418, "WA"));
    } finally {
      resetQueryBucketSize();
    }
  }

  @Test
  public void testPaginatingStatsForJoinField() throws IOException {
    try {
      setQueryBucketSize(2);
      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | stats sum(balance) as a by state | join type=inner state "
                      + "[ source = %s | stats sum(balance) as a by state ]",
                  TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));
      verifyDataRows(
          response,
          rows(39225, "IL"),
          rows(48086, "IN"),
          rows(4180, "MD"),
          rows(40540, "PA"),
          rows(5686, "TN"),
          rows(32838, "VA"),
          rows(16418, "WA"));
    } finally {
      resetQueryBucketSize();
    }
  }

  @Test
  public void testPaginatingStatsForHeadFrom() throws IOException {
    try {
      setQueryBucketSize(2);
      JSONObject response =
          executeQuery(
              String.format(
                  "source=%s | stats sum(balance) as a by state | sort - a | head 5 from 2",
                  TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));
      verifyDataRows(
          response,
          rows(710408, "MA"),
          rows(709135, "TN"),
          rows(657957, "ID"),
          rows(648774, "IL"),
          rows(643489, "AL"));
    } finally {
      resetQueryBucketSize();
    }
  }
}
