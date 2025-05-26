/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

public class MergeRuleUtils {
  public enum MergeRule {
    NO_CHANGE,
    LATEST,
    DEEP_MERGE;

    public static boolean needMerge(MergeRule ruleFirst, MergeRule ruleSecond) {
      return ruleFirst == DEEP_MERGE && ruleSecond == DEEP_MERGE;
    }
  }

  public static MergeRule getMergeRule(ExprCoreType type) {
    switch (type) {
      case STRUCT:
      case ARRAY:
        return MergeRule.DEEP_MERGE;
      case STRING:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case DATE:
        return MergeRule.NO_CHANGE;
      default:
        return MergeRule.LATEST; // dafulat value
    }
  }

  /**
   * The function check whether the two DataType need to be merged if they are under same key.
   * currently we only merge nested and object
   *
   * @param first the
   * @param second
   * @return
   */
  public static Boolean checkWhetherToMerge(OpenSearchDataType first, OpenSearchDataType second) {
    MergeRule mergeRuleFirst = getMergeRule(first.getExprCoreType());
    MergeRule mergeRuleSecond = getMergeRule(second.getExprCoreType());
    if (mergeRuleFirst == MergeRule.LATEST || mergeRuleSecond == MergeRule.LATEST) {
      return false;
    }
    if (first.getExprCoreType() == second.getExprCoreType()
        && MergeRule.needMerge(mergeRuleFirst, mergeRuleSecond)) {
      return true;
    }
    return false;
  }
}
