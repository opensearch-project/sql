/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan.context;

import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;

import java.util.List;
import joptsimple.internal.Strings;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelFieldCollation;

@EqualsAndHashCode
@RequiredArgsConstructor
public class RareTopDigest {
  private final String target;
  private final List<String> byList;
  private final Integer number;
  private final RelFieldCollation.Direction direction;

  public String target() {
    return target;
  }

  public  List<String> byList() {
    return byList;
  }

  public Integer number() {
    return number;
  }

  public  RelFieldCollation.Direction direction() {
    return direction;
  }

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
