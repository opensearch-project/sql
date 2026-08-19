/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.Capability.LEGACY_ENGINE_FALLBACK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

/**
 * Execution coverage for {@code date_histogram} and {@code histogram}. The expander unit tests
 * assert the AST that gets built; these assert what comes back after analysis, planning and
 * pushdown, against 72 documents on fixed timestamps:
 *
 * <pre>
 *   00:00 x5 alpha   00:30 x7 beta    01:00 x11 alpha
 *   01:45 x13 gamma  02:00 x17 beta   03:00 x19 alpha
 * </pre>
 *
 * so hourly grouping must yield 12/24/17/19 and half-hourly 5/7/11/13/17/19.
 */
public class DateHistogramBucketFunctionIT extends SQLIntegTestCase {

  private static final String IDX = "date_histogram_test";

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.DATE_HISTOGRAM_TEST);
  }

  /** The planner rejects {@code GROUP BY <expression>}, so the bucket is aliased in a subquery. */
  private static String bucketed(String bucketExpr) {
    return "SELECT b, COUNT(*) FROM (SELECT "
        + bucketExpr
        + " AS b FROM "
        + IDX
        + ") sub GROUP BY b ORDER BY b";
  }

  @Test
  public void hourlyBucketsCarryKeysAndCounts() throws IOException {
    JSONObject response = executeQuery(bucketed("date_histogram('field'=ts, 'interval'='1h')"));

    verifyDataRowsInOrder(
        response,
        rows("2026-01-01 00:00:00", 12),
        rows("2026-01-01 01:00:00", 24),
        rows("2026-01-01 02:00:00", 17),
        rows("2026-01-01 03:00:00", 19));
  }

  /** A sub-hour interval must split 00:00/00:30 and 01:00/01:45 rather than merge them. */
  @Test
  public void halfHourlyBucketsSplitWithinTheHour() throws IOException {
    JSONObject response = executeQuery(bucketed("date_histogram('field'=ts, 'interval'='30m')"));

    verifyDataRowsInOrder(
        response,
        rows("2026-01-01 00:00:00", 5),
        rows("2026-01-01 00:30:00", 7),
        rows("2026-01-01 01:00:00", 11),
        rows("2026-01-01 01:30:00", 13),
        rows("2026-01-01 02:00:00", 17),
        rows("2026-01-01 03:00:00", 19));
  }

  @Test
  public void dailyIntervalCollapsesEverythingIntoOneBucket() throws IOException {
    JSONObject response = executeQuery(bucketed("date_histogram('field'=ts, 'interval'='1d')"));

    verifyDataRows(response, rows("2026-01-01 00:00:00", 72));
  }

  /** {@code fixed_interval} and {@code calendar_interval} are accepted as synonyms of interval. */
  @Test
  public void intervalSynonymsProduceTheSameBuckets() throws IOException {
    JSONObject viaFixed =
        executeQuery(bucketed("date_histogram('field'=ts, 'fixed_interval'='1h')"));
    JSONObject viaCalendar =
        executeQuery(bucketed("date_histogram('field'=ts, 'calendar_interval'='1h')"));

    for (JSONObject response : new JSONObject[] {viaFixed, viaCalendar}) {
      verifyDataRowsInOrder(
          response,
          rows("2026-01-01 00:00:00", 12),
          rows("2026-01-01 01:00:00", 24),
          rows("2026-01-01 02:00:00", 17),
          rows("2026-01-01 03:00:00", 19));
    }
  }

  /**
   * The scan sits in its own derived table because the V2 engine cannot resolve the span's field
   * otherwise when a second grouping key is present.
   */
  @Test
  public void bucketsCombineWithAnAdditionalGroupingKey() throws IOException {
    JSONObject response =
        executeQuery(
            "SELECT b, c, COUNT(*) FROM (SELECT date_histogram('field'=ts, 'interval'='1h') AS b,"
                + " category AS c FROM (SELECT * FROM "
                + IDX
                + ") inner_scan) sub GROUP BY b, c ORDER BY b, c");

    verifyDataRowsInOrder(
        response,
        rows("2026-01-01 00:00:00", "alpha", 5),
        rows("2026-01-01 00:00:00", "beta", 7),
        rows("2026-01-01 01:00:00", "alpha", 11),
        rows("2026-01-01 01:00:00", "gamma", 13),
        rows("2026-01-01 02:00:00", "beta", 17),
        rows("2026-01-01 03:00:00", "alpha", 19));
  }

  @Test
  public void bucketsRespectAWhereClause() throws IOException {
    JSONObject response =
        executeQuery(
            "SELECT b, COUNT(*) FROM (SELECT date_histogram('field'=ts, 'interval'='1h') AS b FROM "
                + IDX
                + " WHERE category = 'alpha') sub GROUP BY b ORDER BY b");

    verifyDataRowsInOrder(
        response,
        rows("2026-01-01 00:00:00", 5),
        rows("2026-01-01 01:00:00", 11),
        rows("2026-01-01 03:00:00", 19));
  }

  /**
   * `missing` substitutes a value for a null field before bucketing. Asserted end to end because
   * the AST alone cannot show whether the substitution function is one the engine evaluates —
   * `coalesce` is a registered name with no V2 implementation, `ifnull` is the one that runs.
   *
   * <p>Uses the numeric field: substituting into a date needs a timestamp-typed replacement, and
   * the grammar admits only literals here, so a date `missing` reaches IFNULL as TIMESTAMP against
   * STRING. V2 accepts that pair, the analytics engine rejects it, and a test asserting either
   * result would disagree with the other route.
   */
  @Test
  public void missingParameterSubstitutesBeforeBucketing() throws IOException {
    JSONObject response =
        executeQuery(
            "SELECT b, COUNT(*) FROM (SELECT histogram('field'=value, 'interval'=20, 'missing'=0)"
                + " AS b FROM "
                + IDX
                + ") sub GROUP BY b ORDER BY b");

    verifyDataRowsInOrder(response, rows(0, 19), rows(20, 20), rows(40, 20), rows(60, 13));
  }

  @Test
  public void numericHistogramBucketsByInterval() throws IOException {
    JSONObject response =
        executeQuery(
            "SELECT b, COUNT(*) FROM (SELECT histogram('field'=value, 'interval'=20) AS b FROM "
                + IDX
                + ") sub GROUP BY b ORDER BY b");

    // value runs 1..72, so the 20-wide buckets hold 19, 20, 20 and 13 documents.
    verifyDataRowsInOrder(response, rows(0, 19), rows(20, 20), rows(40, 20), rows(60, 13));
  }

  /**
   * Only the legacy V1 engine understands this spelling, and it answered before these names entered
   * the V2 grammar. The expander has to keep declining with SyntaxCheckException so it still does.
   */
  @Test
  @RequiresCapability(LEGACY_ENGINE_FALLBACK)
  public void positionalCallReturnsHourlyBuckets() throws IOException {
    JSONObject response =
        executeQuery(
            "SELECT COUNT(*) FROM " + IDX + " GROUP BY date_histogram(field='ts','interval'='1h')");

    verifyDataRows(response, rows(12), rows(24), rows(17), rows(19));
  }

  /**
   * `alias` has no lowering here but the legacy engine implements it, so the query still has to
   * answer. CsvFormatResponseIT.dateHistogramTest has asserted this shape for years.
   */
  @Test
  @RequiresCapability(LEGACY_ENGINE_FALLBACK)
  public void callWithAliasParameterReturnsHourlyBuckets() throws IOException {
    JSONObject response =
        executeQuery(
            "SELECT COUNT(*) FROM "
                + IDX
                + " GROUP BY date_histogram('field'='ts','fixed_interval'='1h','alias'='hours')");

    verifyDataRows(response, rows(12), rows(24), rows(17), rows(19));
  }

  @Test
  @RequiresCapability(LEGACY_ENGINE_FALLBACK)
  public void positionalNumericHistogramReturnsBuckets() throws IOException {
    JSONObject response =
        executeQuery(
            "SELECT COUNT(*) FROM " + IDX + " GROUP BY histogram(field='value','interval'='20')");

    verifyDataRows(response, rows(19), rows(20), rows(20), rows(13));
  }
}
