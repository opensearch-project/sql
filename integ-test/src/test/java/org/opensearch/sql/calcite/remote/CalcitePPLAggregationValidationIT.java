/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_LOGS;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLAggregationValidationIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.LOGS);
  }

  @Test
  public void testCountWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats count(balance, age)"),
        "Aggregation function COUNT expects field type and additional arguments {[]|[ANY]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testAvgWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats avg(balance, age)"),
        "Aggregation function AVG expects field type and additional arguments {[INTEGER]|[DOUBLE]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testSumWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats sum(balance, age)"),
        "Aggregation function SUM expects field type and additional arguments {[INTEGER]|[DOUBLE]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testMinWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats min(balance, age)"),
        "Aggregation function MIN expects field type and additional arguments {[COMPARABLE_TYPE]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testMaxWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats max(balance, age)"),
        "Aggregation function MAX expects field type and additional arguments {[COMPARABLE_TYPE]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testVarSampWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats var_samp(balance, age)"),
        "Aggregation function VARSAMP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [LONG,INTEGER]");
  }

  @Test
  public void testVarPopWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats var_pop(balance, age)"),
        "Aggregation function VARPOP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [LONG,INTEGER]");
  }

  @Test
  public void testStddevSampWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats stddev_samp(balance, age)"),
        "Aggregation function STDDEV_SAMP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [LONG,INTEGER]");
  }

  @Test
  public void testStddevPopWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats stddev_pop(balance, age)"),
        "Aggregation function STDDEV_POP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [LONG,INTEGER]");
  }

  @Test
  public void testPercentileWithMissingParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats percentile(balance)"),
        "Aggregation function PERCENTILE_APPROX expects field type"
            + " {[INTEGER,INTEGER]|[INTEGER,DOUBLE]|[DOUBLE,INTEGER]|[DOUBLE,DOUBLE]|[INTEGER,INTEGER,INTEGER]|[INTEGER,INTEGER,DOUBLE]|[INTEGER,DOUBLE,INTEGER]|[INTEGER,DOUBLE,DOUBLE]|[DOUBLE,INTEGER,INTEGER]|[DOUBLE,INTEGER,DOUBLE]|[DOUBLE,DOUBLE,INTEGER]|[DOUBLE,DOUBLE,DOUBLE]},"
            + " but got [LONG]");
  }

  @Test
  public void testPercentileWithInvalidParameterTypesThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "stats percentile(balance, 50, firstname)"),
        "Aggregation function PERCENTILE_APPROX expects field type and additional arguments"
            + " {[INTEGER,INTEGER]|[INTEGER,DOUBLE]|[DOUBLE,INTEGER]|[DOUBLE,DOUBLE]|[INTEGER,INTEGER,INTEGER]|[INTEGER,INTEGER,DOUBLE]|[INTEGER,DOUBLE,INTEGER]|[INTEGER,DOUBLE,DOUBLE]|[DOUBLE,INTEGER,INTEGER]|[DOUBLE,INTEGER,DOUBLE]|[DOUBLE,DOUBLE,INTEGER]|[DOUBLE,DOUBLE,DOUBLE]},"
            + " but got [LONG,INTEGER,STRING]");
  }

  @Test
  public void testEarliestWithTooManyParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_LOGS, "stats earliest(server, @timestamp, message)"),
        "Aggregation function EARLIEST expects field type and additional arguments"
            + " {[ANY]|[ANY,ANY]}, but got"
            + " [STRING,TIMESTAMP,STRING]");
  }

  @Test
  public void testLatestWithTooManyParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_LOGS, "stats latest(server, @timestamp, message)"),
        "Aggregation function LATEST expects field type and additional arguments"
            + " {[ANY]|[ANY,ANY]}, but got"
            + " [STRING,TIMESTAMP,STRING]");
  }
}
