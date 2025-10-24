/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.calcite.rex.RexNode;


@EqualsAndHashCode
public class FilterDigest {
  private final int scriptCount;
  private final RexNode condition;

  public FilterDigest(int scriptCount, RexNode condition) {
    this.scriptCount = scriptCount;
    this.condition = condition;
  }

  public int scriptCount() {
    return scriptCount;
  }

  public RexNode condition() {
    return condition;
  }

  @Override
  public String toString() {
    return condition.toString();
  }
}
