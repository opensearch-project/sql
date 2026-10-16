/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.Map;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchKnnVectorType;

/**
 * Merges a knn_vector field that several indices map, as {@link DeepMergeRule} merged it when
 * knn_vector mapped to ARRAY: the sub-field properties of the source (a knn_vector field can hold
 * those of a nested field merged into it) are merged into the accumulated type's, and that type is
 * kept, now as {@link OpenSearchKnnVectorType#mergedWith}, which only claims what all the mappings
 * share. SQL and the V2 engine read the field the same way either way.
 */
public class VectorMergeRule implements MergeRule {

  @Override
  public boolean isMatch(OpenSearchDataType source, OpenSearchDataType target) {
    return source instanceof OpenSearchKnnVectorType && target instanceof OpenSearchKnnVectorType;
  }

  @Override
  public void mergeInto(
      String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target) {
    OpenSearchKnnVectorType existing = (OpenSearchKnnVectorType) target.get(key);
    MergeRuleHelper.merge(existing.getProperties(), source.getProperties());
    target.put(key, existing.mergedWith((OpenSearchKnnVectorType) source));
  }
}
