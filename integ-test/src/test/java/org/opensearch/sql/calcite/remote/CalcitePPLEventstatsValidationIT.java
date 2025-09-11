/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.*;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLEventstatsValidationIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.LOGS);
  }

  @Test
  public void testCountWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats count(balance, age)"),
        "Aggregation function COUNT expects field type and additional arguments {[]|[ANY]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testAvgWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats avg(balance, age)"),
        "Aggregation function AVG expects field type and additional arguments {[INTEGER]|[DOUBLE]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testSumWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats sum(balance, age)"),
        "Aggregation function SUM expects field type and additional arguments {[INTEGER]|[DOUBLE]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testMinWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats min(balance, age)"),
        "Aggregation function MIN expects field type and additional arguments {[COMPARABLE_TYPE]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testMaxWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats max(balance, age)"),
        "Aggregation function MAX expects field type and additional arguments {[COMPARABLE_TYPE]},"
            + " but got [LONG,INTEGER]");
  }

  @Test
  public void testVarSampWithExtraParametersThrowsException() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats var_samp(balance, age)"),
        "Aggregation function VARSAMP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [LONG,INTEGER]");
  }

  @Test
  public void testVarPopWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats var_pop(balance, age)"),
        "Aggregation function VARPOP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [LONG,INTEGER]");
  }

  @Test
  public void testStddevSampWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats stddev_samp(balance, age)"),
        "Aggregation function STDDEV_SAMP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [LONG,INTEGER]");
  }

  @Test
  public void testStddevPopWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_BANK, "eventstats stddev_pop(balance, age)"),
        "Aggregation function STDDEV_POP expects field type and additional arguments"
            + " {[INTEGER]|[DOUBLE]}, but got [LONG,INTEGER]");
  }

  @Test
  public void testEarliestWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_LOGS, "eventstats earliest(server, @timestamp, message)"),
        "Aggregation function EARLIEST expects field type and additional arguments"
            + " {[ANY]|[ANY,ANY]}, but got"
            + " [STRING,TIMESTAMP,STRING]");
  }

  @Test
  public void testLatestWithInvalidParams() throws IOException {
    verifyExplainException(
        source(TEST_INDEX_LOGS, "eventstats latest(server, @timestamp, message)"),
        "Aggregation function LATEST expects field type and additional arguments"
            + " {[ANY]|[ANY,ANY]}, but got"
            + " [STRING,TIMESTAMP,STRING]");
  }
}
