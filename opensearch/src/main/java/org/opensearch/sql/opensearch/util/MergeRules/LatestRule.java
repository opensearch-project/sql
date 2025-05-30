/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.Map;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;

/** The rule always keep the latest one. */
public class LatestRule implements MergeRule {

  @Override
  public boolean isMatch(OpenSearchDataType source, OpenSearchDataType target) {
    return true;
  }

  @Override
  public void mergeInto(
      String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target) {
    target.put(key, source);
  }
}
