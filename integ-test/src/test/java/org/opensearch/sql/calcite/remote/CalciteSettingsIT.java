/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import java.io.IOException;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.ppl.SettingsIT;

public class CalciteSettingsIT extends SettingsIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Override
  public void testQuerySizeLimit_NoPushdown() throws IOException {
    withSettings(
        Key.CALCITE_PUSHDOWN_ENABLED,
        "false",
        () -> {
          try {
            super.testQuerySizeLimit_NoPushdown();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
