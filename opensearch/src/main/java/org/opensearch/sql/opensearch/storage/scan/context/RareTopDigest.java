/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;

import java.util.List;
import joptsimple.internal.Strings;
import org.apache.calcite.rel.RelFieldCollation;

public record RareTopDigest(
    String target, List<String> byList, Integer number, RelFieldCollation.Direction direction) {

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(direction == ASCENDING ? "rare" : "top");
    builder.append(" ");
    builder.append(number);
    builder.append(" ");
    builder.append(target);
    if (!byList.isEmpty()) {
      builder.append(" by ");
      builder.append(Strings.join(byList, ","));
    }
    return builder.toString();
  }
}
