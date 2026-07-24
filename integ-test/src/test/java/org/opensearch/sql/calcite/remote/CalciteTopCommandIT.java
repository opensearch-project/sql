/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.TopCommandIT;

public class CalciteTopCommandIT extends TopCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testTopCommandUseNull() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | top age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchemaInOrder(result, schema("age", "int"), schema("count", "bigint"));
    verifyNumOfRows(result, 6);
  }

  @Test
  public void testTopCommandUseNullFalse() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | top usenull=false age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchemaInOrder(result, schema("age", "int"), schema("count", "bigint"));
    verifyNumOfRows(result, 5);
  }

  @Test
  public void testTopCommandShowPerc() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | top showperc=true age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchemaInOrder(
        result, schema("age", "int"), schema("count", "bigint"), schema("percent", "double"));
    verifyNumOfRows(result, 6);
    verifyDataRows(
        result,
        rows(36, 2, 28.57),
        rows(28, 1, 14.29),
        rows(32, 1, 14.29),
        rows(33, 1, 14.29),
        rows(34, 1, 14.29),
        rows(null, 1, 14.29));
  }

  @Test
  public void testTopCommandShowPercWithoutShowCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | top showperc=true showcount=false age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchemaInOrder(result, schema("age", "int"), schema("percent", "double"));
    verifyNumOfRows(result, 6);
    verifyDataRows(
        result,
        rows(36, 28.57),
        rows(28, 14.29),
        rows(32, 14.29),
        rows(33, 14.29),
        rows(34, 14.29),
        rows(null, 14.29));
  }

  @Test
  public void testTopCommandLegacyFalse() throws IOException {
    withSettings(
        Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "false",
        () -> {
          JSONObject result;
          try {
            result =
                executeQuery(
                    String.format("source=%s | top age", TEST_INDEX_BANK_WITH_NULL_VALUES));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          verifySchemaInOrder(result, schema("age", "int"), schema("count", "bigint"));
          verifyNumOfRows(result, 5);
        });
  }
}
