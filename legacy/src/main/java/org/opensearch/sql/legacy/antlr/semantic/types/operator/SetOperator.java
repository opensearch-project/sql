/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.operator;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;

import java.util.List;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;

/** Set operator between queries. */
public enum SetOperator implements Type {
  UNION,
  MINUS,
  IN;

  @Override
  public String getName() {
    return name();
  }

  @Override
  public Type construct(List<Type> others) {
    if (others.size() < 2) {
      throw new IllegalStateException("");
    }

    // Compare each type and return anyone for now if pass
    for (int i = 0; i < others.size() - 1; i++) {
      Type type1 = others.get(i);
      Type type2 = others.get(i + 1);

      // Do it again as in Product because single base type won't be wrapped in Product
      if (!type1.isCompatible(type2) && !type2.isCompatible(type1)) {
        return TYPE_ERROR;
      }
    }
    return others.get(0);
  }

  @Override
  public String usage() {
    return "Please return field(s) of compatible type from each query.";
  }

  @Override
  public String toString() {
    return "Operator [" + getName() + "]";
  }
}
