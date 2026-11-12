/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.Map;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;

/**
 * Merge rule for object/scalar type conflicts across indices. When a path is an object (or nested)
 * in one index and a scalar in another -- typically after a mapping change at a rollover boundary
 * -- the path resolves to the scalar type.
 *
 * <p>Without this rule the pair matches no other rule and falls through to {@link LatestRule}, so
 * the winner is whichever index is merged last. That order comes from the mapping map built with
 * {@code Collectors.toUnmodifiableMap}, whose iteration order the JDK randomizes per JVM, making
 * the resolved type differ between nodes and change across restarts.
 *
 * <p>The scalar side wins because it is the only side with a value that can be grouped, sorted or
 * charted, and it keeps doc-values pushdown available. Documents from the indices that map the path
 * as an object have no scalar there, so they aggregate into the missing bucket. The object's
 * sub-fields are dropped along with it: the row cannot hold both a scalar and a subtree at one
 * path, so keeping them would resolve `path.sub` to a column that always reads null. Failing such a
 * query with "field not found" is the honest outcome; querying the object-mapped index directly
 * still returns the sub-fields.
 *
 * <p>See GitHub issue #5752.
 */
public class ObjectScalarConflictRule implements MergeRule {

  @Override
  public boolean isMatch(OpenSearchDataType source, OpenSearchDataType target) {
    if (source == null || target == null) {
      return false;
    }
    return (isContainer(source) && isScalar(target)) || (isScalar(source) && isContainer(target));
  }

  @Override
  public void mergeInto(
      String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target) {
    OpenSearchDataType scalar = isContainer(source) ? target.get(key) : source;
    target.put(key, scalar);
  }

  /** An object or nested type, i.e. one whose value is a subtree rather than a single value. */
  private static boolean isContainer(OpenSearchDataType type) {
    ExprCoreType coreType = type.getExprCoreType();
    return coreType == ExprCoreType.STRUCT || coreType == ExprCoreType.ARRAY;
  }

  /**
   * A single-valued type. Decided on the mapping type rather than the {@link ExprCoreType}, because
   * text, match_only_text, geo_point and binary all resolve to {@link ExprCoreType#UNKNOWN} while
   * still holding one value per document. An alias only redirects to another path, so it is left to
   * the other rules.
   */
  private static boolean isScalar(OpenSearchDataType type) {
    MappingType mappingType = type.getMappingType();
    return !isContainer(type)
        && mappingType != MappingType.Alias
        && mappingType != MappingType.Invalid;
  }
}
