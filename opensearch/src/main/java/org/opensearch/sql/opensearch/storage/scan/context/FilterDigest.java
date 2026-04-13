/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import java.util.Set;
import org.apache.calcite.rex.RexNode;

public record FilterDigest(int scriptCount, RexNode condition, Set<Integer> notNullFieldIndices) {

  public FilterDigest(int scriptCount, RexNode condition) {
    this(scriptCount, condition, Set.of());
  }

  @Override
  public String toString() {
    return condition.toString();
  }
}
