/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.Map;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

/**
 * The Interface to merge index schemas. Need to implement isMatch: Whether match this rule,
 * mergeInto, how to merge the source type to target map.
 */
public interface MergeRule {
  boolean isMatch(OpenSearchDataType source, OpenSearchDataType target);

  void mergeInto(String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target);
}
