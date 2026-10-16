/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.Map;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;

/** This rule will merge two array/struct object and merge their properties */
public class DeepMergeRule implements MergeRule {

  @Override
  public boolean isMatch(OpenSearchDataType source, OpenSearchDataType target) {
    return source != null
        && target != null
        && mergeCoreType(source) == mergeCoreType(target)
        && (mergeCoreType(source) == ExprCoreType.STRUCT
            || mergeCoreType(source) == ExprCoreType.ARRAY);
  }

  /**
   * The core type a field merges by. A knn_vector merges as the ARRAY it mapped to before VECTOR
   * existed, so that it is still deep-merged with a nested field of the same name, keeping the
   * first index's type, as before.
   */
  private static ExprCoreType mergeCoreType(OpenSearchDataType type) {
    return type instanceof OpenSearchKnnVectorType ? ExprCoreType.ARRAY : type.getExprCoreType();
  }

  @Override
  public void mergeInto(
      String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target) {
    OpenSearchDataType existing = target.get(key);
    MergeRuleHelper.merge(existing.getProperties(), source.getProperties());
    target.put(key, existing);
  }
}
