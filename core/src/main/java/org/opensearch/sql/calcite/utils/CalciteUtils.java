/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_ENGINE_ENABLED;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CalciteUtils {

  public static UnsupportedOperationException getOnlyForCalciteException(String feature) {
    return new UnsupportedOperationException(
        feature + " is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true");
  }
}
