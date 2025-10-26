/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import org.apache.calcite.rex.RexNode;

public record FilterDigest(int scriptCount, RexNode condition) {
  @Override
  public String toString() {
    return condition.toString();
  }
}
