/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.operator;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;

import java.util.List;
import java.util.Optional;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex;

/** Join operator */
public enum JoinOperator implements Type {
  JOIN;

  @Override
  public String getName() {
    return name();
  }

  @Override
  public Type construct(List<Type> others) {
    Optional<Type> isAnyNonIndexType =
        others.stream().filter(type -> !(type instanceof OpenSearchIndex)).findAny();
    if (isAnyNonIndexType.isPresent()) {
      return TYPE_ERROR;
    }
    return others.get(0);
  }

  @Override
  public String usage() {
    return "Please join index with other index or its nested field.";
  }

  @Override
  public String toString() {
    return "Operator [" + getName() + "]";
  }
}
