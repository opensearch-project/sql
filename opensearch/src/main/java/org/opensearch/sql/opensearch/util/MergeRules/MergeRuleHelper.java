/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

public class MergeRuleHelper {
  private static final List<MergeRule> RULES =
      List.of(
          new DeepMergeRule(), new LatestRule() // must come last
          );

  public static MergeRule selectRule(OpenSearchDataType source, OpenSearchDataType target) {
    return RULES.stream()
        .filter(rule -> rule.isMatch(source, target))
        .findFirst()
        .orElseThrow(); // logically unreachable if fallback exists
  }

  public static void merge(
      Map<String, OpenSearchDataType> target, Map<String, OpenSearchDataType> source) {
    for (Map.Entry<String, OpenSearchDataType> entry : source.entrySet()) {
      String key = entry.getKey();
      OpenSearchDataType sourceValue = entry.getValue();
      OpenSearchDataType targetValue = target.get(key);
      MergeRuleHelper.selectRule(sourceValue, targetValue).mergeInto(key, sourceValue, target);
    }
  }
}
