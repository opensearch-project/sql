/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.RareCommandIT;

public class CalciteRareCommandIT extends RareCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testRareCommandUseNull() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | rare age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchemaInOrder(result, schema("age", "int"), schema("count", "bigint"));
    verifyNumOfRows(result, 6);
  }

  @Test
  public void testRareCommandUseNullFalse() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | rare usenull=false age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchemaInOrder(result, schema("age", "int"), schema("count", "bigint"));
    verifyNumOfRows(result, 5);
  }

  @Test
  public void testRareCommandLegacyFalse() throws IOException {
    withSettings(
        Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "false",
        () -> {
          JSONObject result;
          try {
            result =
                executeQuery(
                    String.format("source=%s | rare age", TEST_INDEX_BANK_WITH_NULL_VALUES));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          verifySchemaInOrder(result, schema("age", "int"), schema("count", "bigint"));
          verifyNumOfRows(result, 5);
        });
  }
}
