/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.common.setting.Settings.Key.CALCITE_ENGINE_ENABLED;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@UtilityClass
public class CalciteUtils {

  public static UnsupportedOperationException getOnlyForCalciteException(String feature) {
    return new UnsupportedOperationException(
        feature + " is supported only when " + CALCITE_ENGINE_ENABLED.getKeyValue() + "=true");
  }

  public static <T> Pair<List<T>, List<T>> partition(
      Collection<T> collection, Predicate<T> predicate) {
    Map<Boolean, List<T>> map = collection.stream().collect(Collectors.partitioningBy(predicate));
    return new ImmutablePair<>(map.get(true), map.get(false));
  }
}
