/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.planner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.opensearch.sql.legacy.util.MatcherUtils.featureValueOf;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.opensearch.sql.legacy.domain.hints.Hint;
import org.opensearch.sql.legacy.domain.hints.HintFactory;
import org.opensearch.sql.legacy.domain.hints.HintType;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.query.planner.HashJoinQueryPlanRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.Config;

/** Hint & Configuring Ability Test Cases */
public class QueryPlannerConfigTest extends QueryPlannerTest {

  private static final Matcher<Integer[]> DEFAULT_TOTAL_AND_TABLE_LIMIT_MATCHER =
      totalAndTableLimit(200, 0, 0);

  @Test
  public void algorithmBlockSizeHint() {
    assertThat(
        parseHint("! JOIN_ALGORITHM_BLOCK_SIZE(100000)"),
        hint(hintType(HintType.JOIN_ALGORITHM_BLOCK_SIZE), hintValues(100000)));
  }

  @Test
  public void algorithmUseLegacy() {
    assertThat(
        parseHint("! JOIN_ALGORITHM_USE_LEGACY"),
        hint(hintType(HintType.JOIN_ALGORITHM_USE_LEGACY), hintValues()));
  }

  @Test
  public void algorithmBlockSizeHintWithSpaces() {
    assertThat(
        parseHint("! JOIN_ALGORITHM_BLOCK_SIZE ( 200000 ) "),
        hint(hintType(HintType.JOIN_ALGORITHM_BLOCK_SIZE), hintValues(200000)));
  }

  @Test
  public void scrollPageSizeHint() {
    assertThat(
        parseHint("! JOIN_SCROLL_PAGE_SIZE(1000) "),
        hint(hintType(HintType.JOIN_SCROLL_PAGE_SIZE), hintValues(1000)));
  }

  @Test
  public void scrollPageSizeHintWithTwoSizes() {
    assertThat(
        parseHint("! JOIN_SCROLL_PAGE_SIZE(1000, 2000) "),
        hint(hintType(HintType.JOIN_SCROLL_PAGE_SIZE), hintValues(1000, 2000)));
  }

  @Test
  public void circuitBreakLimitHint() {
    assertThat(
        parseHint("! JOIN_CIRCUIT_BREAK_LIMIT(80)"),
        hint(hintType(HintType.JOIN_CIRCUIT_BREAK_LIMIT), hintValues(80)));
  }

  @Test
  public void backOffRetryIntervalsHint() {
    assertThat(
        parseHint("! JOIN_BACK_OFF_RETRY_INTERVALS(1, 5)"),
        hint(hintType(HintType.JOIN_BACK_OFF_RETRY_INTERVALS), hintValues(1, 5)));
  }

  @Test
  public void timeOutHint() {
    assertThat(
        parseHint("! JOIN_TIME_OUT(120)"), hint(hintType(HintType.JOIN_TIME_OUT), hintValues(120)));
  }

  @Test
  public void blockSizeConfig() {
    assertThat(
        queryPlannerConfig(
            "SELECT /*! JOIN_ALGORITHM_BLOCK_SIZE(200000) */ "
                + "  d.name FROM employee e JOIN department d ON d.id = e.departmentId "),
        config(
            blockSize(200000),
            scrollPageSize(Config.DEFAULT_SCROLL_PAGE_SIZE, Config.DEFAULT_SCROLL_PAGE_SIZE),
            circuitBreakLimit(Config.DEFAULT_CIRCUIT_BREAK_LIMIT),
            backOffRetryIntervals(Config.DEFAULT_BACK_OFF_RETRY_INTERVALS),
            DEFAULT_TOTAL_AND_TABLE_LIMIT_MATCHER,
            timeOut(Config.DEFAULT_TIME_OUT)));
  }

  @Test
  public void scrollPageSizeConfig() {
    assertThat(
        queryPlannerConfig(
            "SELECT /*! JOIN_SCROLL_PAGE_SIZE(50, 20) */ "
                + "  d.name FROM employee e JOIN department d ON d.id = e.departmentId "),
        config(
            blockSize(Config.DEFAULT_BLOCK_SIZE),
            scrollPageSize(50, 20),
            circuitBreakLimit(Config.DEFAULT_CIRCUIT_BREAK_LIMIT),
            backOffRetryIntervals(Config.DEFAULT_BACK_OFF_RETRY_INTERVALS),
            DEFAULT_TOTAL_AND_TABLE_LIMIT_MATCHER,
            timeOut(Config.DEFAULT_TIME_OUT)));
  }

  @Test
  public void circuitBreakLimitConfig() {
    assertThat(
        queryPlannerConfig(
            "SELECT /*! JOIN_CIRCUIT_BREAK_LIMIT(60) */ "
                + "  d.name FROM employee e JOIN department d ON d.id = e.departmentId "),
        config(
            blockSize(Config.DEFAULT_BLOCK_SIZE),
            scrollPageSize(Config.DEFAULT_SCROLL_PAGE_SIZE, Config.DEFAULT_SCROLL_PAGE_SIZE),
            circuitBreakLimit(60),
            backOffRetryIntervals(Config.DEFAULT_BACK_OFF_RETRY_INTERVALS),
            DEFAULT_TOTAL_AND_TABLE_LIMIT_MATCHER,
            timeOut(Config.DEFAULT_TIME_OUT)));
  }

  @Test
  public void backOffRetryIntervalsConfig() {
    assertThat(
        queryPlannerConfig(
            "SELECT /*! JOIN_BACK_OFF_RETRY_INTERVALS(1, 3, 5, 10) */ "
                + "  d.name FROM employee e JOIN department d ON d.id = e.departmentId "),
        config(
            blockSize(Config.DEFAULT_BLOCK_SIZE),
            scrollPageSize(Config.DEFAULT_SCROLL_PAGE_SIZE, Config.DEFAULT_SCROLL_PAGE_SIZE),
            circuitBreakLimit(Config.DEFAULT_CIRCUIT_BREAK_LIMIT),
            backOffRetryIntervals(new double[] {1, 3, 5, 10}),
            DEFAULT_TOTAL_AND_TABLE_LIMIT_MATCHER,
            timeOut(Config.DEFAULT_TIME_OUT)));
  }

  @Test
  public void totalAndTableLimitConfig() {
    assertThat(
        queryPlannerConfig(
            "SELECT /*! JOIN_TABLES_LIMIT(10, 20) */ "
                + "  d.name FROM employee e JOIN department d ON d.id = e.departmentId LIMIT 50"),
        config(
            blockSize(Config.DEFAULT_BLOCK_SIZE),
            scrollPageSize(Config.DEFAULT_SCROLL_PAGE_SIZE, Config.DEFAULT_SCROLL_PAGE_SIZE),
            circuitBreakLimit(Config.DEFAULT_CIRCUIT_BREAK_LIMIT),
            backOffRetryIntervals(Config.DEFAULT_BACK_OFF_RETRY_INTERVALS),
            totalAndTableLimit(50, 10, 20),
            timeOut(Config.DEFAULT_TIME_OUT)));
  }

  @Test
  public void timeOutConfig() {
    assertThat(
        queryPlannerConfig(
            "SELECT /*! JOIN_TIME_OUT(120) */ "
                + "  d.name FROM employee e JOIN department d ON d.id = e.departmentId"),
        config(
            blockSize(Config.DEFAULT_BLOCK_SIZE),
            scrollPageSize(Config.DEFAULT_SCROLL_PAGE_SIZE, Config.DEFAULT_SCROLL_PAGE_SIZE),
            circuitBreakLimit(Config.DEFAULT_CIRCUIT_BREAK_LIMIT),
            backOffRetryIntervals(Config.DEFAULT_BACK_OFF_RETRY_INTERVALS),
            DEFAULT_TOTAL_AND_TABLE_LIMIT_MATCHER,
            timeOut(120)));
  }

  @Test
  public void multipleConfigCombined() {
    assertThat(
        queryPlannerConfig(
            "SELECT "
                + "  /*! JOIN_ALGORITHM_BLOCK_SIZE(100) */ "
                + "  /*! JOIN_SCROLL_PAGE_SIZE(50, 20) */ "
                + "  /*! JOIN_CIRCUIT_BREAK_LIMIT(10) */ "
                + "  d.name FROM employee e JOIN department d ON d.id = e.departmentId "),
        config(
            blockSize(100),
            scrollPageSize(50, 20),
            circuitBreakLimit(10),
            backOffRetryIntervals(Config.DEFAULT_BACK_OFF_RETRY_INTERVALS),
            DEFAULT_TOTAL_AND_TABLE_LIMIT_MATCHER,
            timeOut(Config.DEFAULT_TIME_OUT)));
  }

  // ============================================================================
  // Enhanced JOIN_TIME_OUT Configuration Tests
  // ============================================================================

  @Test
  public void testConfigureTimeOutMethodDirectly() {
    Config config = new Config();

    // Test direct method call
    config.configureTimeOut(new Object[] {180});
    assertEquals("Timeout should be set via configureTimeOut method", 180, config.timeout());

    // Test with different values
    config.configureTimeOut(new Object[] {300});
    assertEquals("Timeout should be updated", 300, config.timeout());

    // Test with empty array (should not change timeout)
    int previousTimeout = config.timeout();
    config.configureTimeOut(new Object[] {});
    assertEquals("Empty array should not change timeout", previousTimeout, config.timeout());
  }

  @Test
  public void testJoinTimeOutHintOverridesDefault() {
    // Test without hint
    String sqlWithoutHint =
        "SELECT d.name FROM employee e JOIN department d ON d.id = e.departmentId";
    Config configWithoutHint = queryPlannerConfig(sqlWithoutHint);

    // Test with hint
    String sqlWithHint =
        "SELECT /*! JOIN_TIME_OUT(480) */ "
            + "d.name FROM employee e JOIN department d ON d.id = e.departmentId";
    Config configWithHint = queryPlannerConfig(sqlWithHint);

    assertEquals(
        "Without hint should use default", Config.DEFAULT_TIME_OUT, configWithoutHint.timeout());
    assertEquals("With hint should override default", 480, configWithHint.timeout());
    assertNotEquals(
        "Hint should change the timeout value",
        configWithoutHint.timeout(),
        configWithHint.timeout());
  }

  @Test
  public void testJoinTimeOutHintWithVariousValues() {
    int[] timeoutValues = {1, 30, 60, 120, 300, 600, 1800, 3600, 7200};

    for (int timeoutValue : timeoutValues) {
      String sql =
          String.format(
              "SELECT /*! JOIN_TIME_OUT(%d) */ "
                  + "d.name FROM employee e JOIN department d ON d.id = e.departmentId",
              timeoutValue);

      Config config = queryPlannerConfig(sql);
      assertEquals(
          "Timeout value " + timeoutValue + " should be preserved", timeoutValue, config.timeout());
    }
  }

  @Test
  public void testJoinTimeOutHintWithOrderByAndLimit() {
    String sql =
        "SELECT /*! JOIN_TIME_OUT(200) */ "
            + "e.name, d.name FROM employee e JOIN department d ON d.id = e.departmentId "
            + "WHERE e.salary > 50000 "
            + "ORDER BY e.name ASC "
            + "LIMIT 50";

    Config config = queryPlannerConfig(sql);

    assertEquals("Timeout hint should work with complex query", 200, config.timeout());
    assertEquals("Total limit should be set from LIMIT clause", 50, config.totalLimit());
  }

  @Test
  public void testConfigTimeoutImmutabilityAfterSetting() {
    Config config = new Config();

    // Set initial timeout
    config.configureTimeOut(new Object[] {180});
    assertEquals("Initial timeout should be set", 180, config.timeout());

    // Create a new config and verify it has default timeout
    Config newConfig = new Config();
    assertEquals(
        "New config should have default timeout", Config.DEFAULT_TIME_OUT, newConfig.timeout());

    // Original config should still have its timeout
    assertEquals("Original config should retain its timeout", 180, config.timeout());
  }

  @Test
  public void testJoinTimeOutHintValidationRanges() {
    // Test boundary values
    int[] boundaryValues = {
      1, // Minimum practical value
      59, // Just under default
      60, // Default value
      61, // Just over default
      3600, // 1 hour
      86400, // 24 hours
      604800 // 1 week
    };

    for (int timeout : boundaryValues) {
      String sql =
          String.format(
              "SELECT /*! JOIN_TIME_OUT(%d) */ "
                  + "d.name FROM employee e JOIN department d ON d.id = e.departmentId",
              timeout);

      Config config = queryPlannerConfig(sql);
      assertEquals("Boundary value " + timeout + " should be accepted", timeout, config.timeout());
    }
  }

  private Hint parseHint(String hintStr) {
    try {
      return HintFactory.getHintFromString(hintStr);
    } catch (SqlParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private Config queryPlannerConfig(String sql) {
    HashJoinQueryPlanRequestBuilder request =
        ((HashJoinQueryPlanRequestBuilder) createRequestBuilder(sql));
    request.plan();
    return request.getConfig();
  }

  private Matcher<Hint> hint(Matcher<HintType> typeMatcher, Matcher<Object[]> valuesMatcher) {
    return both(featureValueOf("HintType", typeMatcher, Hint::getType))
        .and(featureValueOf("HintValue", valuesMatcher, Hint::getParams));
  }

  private Matcher<HintType> hintType(HintType type) {
    return is(type);
  }

  private Matcher<Object[]> hintValues(Object... values) {
    if (values.length == 0) {
      return emptyArray();
    }
    return arrayContaining(values);
  }

  private Matcher<Config> config(
      Matcher<Integer> blockSizeMatcher,
      Matcher<Integer[]> scrollPageSizeMatcher,
      Matcher<Integer> circuitBreakLimitMatcher,
      Matcher<double[]> backOffRetryIntervalsMatcher,
      Matcher<Integer[]> totalAndTableLimitMatcher,
      Matcher<Integer> timeOutMatcher) {
    return allOf(
        featureValueOf("Block size", blockSizeMatcher, (cfg -> cfg.blockSize().size())),
        featureValueOf("Scroll page size", scrollPageSizeMatcher, Config::scrollPageSize),
        featureValueOf("Circuit break limit", circuitBreakLimitMatcher, Config::circuitBreakLimit),
        featureValueOf(
            "Back off retry intervals",
            backOffRetryIntervalsMatcher,
            Config::backOffRetryIntervals),
        featureValueOf(
            "Total and table limit",
            totalAndTableLimitMatcher,
            (cfg -> new Integer[] {cfg.totalLimit(), cfg.tableLimit1(), cfg.tableLimit2()})),
        featureValueOf("Time out", timeOutMatcher, Config::timeout));
  }

  private Matcher<Integer> blockSize(int size) {
    return is(size);
  }

  @SuppressWarnings("unchecked")
  private Matcher<Integer[]> scrollPageSize(int size1, int size2) {
    return arrayContaining(is(size1), is(size2));
  }

  private Matcher<Integer> circuitBreakLimit(int limit) {
    return is(limit);
  }

  private Matcher<double[]> backOffRetryIntervals(double[] intervals) {
    return is(intervals);
  }

  @SuppressWarnings("unchecked")
  private static Matcher<Integer[]> totalAndTableLimit(
      int totalLimit, int tableLimit1, int tableLimit2) {
    return arrayContaining(is(totalLimit), is(tableLimit1), is(tableLimit2));
  }

  private static Matcher<Integer> timeOut(int timeout) {
    return is(timeout);
  }
}
