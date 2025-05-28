/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.Map;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

/** This rule will merge two array/struct object and merge their properties */
public class DeepMergeRule implements MergeRule {

  @Override
  public boolean isMatch(OpenSearchDataType source, OpenSearchDataType target) {
    return source != null
        && target != null
        && source.getExprCoreType() == target.getExprCoreType()
        && (source.getExprCoreType() == ExprCoreType.STRUCT
            || source.getExprCoreType() == ExprCoreType.ARRAY);
  }

  @Override
  public void mergeInto(
      String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target) {
    OpenSearchDataType existing = target.get(key);
    MergeRuleHelper.merge(existing.getProperties(), source.getProperties());
    target.put(key, existing);
  }
}
