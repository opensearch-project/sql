/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.special;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;

/** Combination of multiple types, ex. function arguments */
public class Product implements Type {

  @Getter private final List<Type> types;

  public Product(List<Type> itemTypes) {
    types = Collections.unmodifiableList(itemTypes);
  }

  @Override
  public String getName() {
    return "Product of types " + types;
  }

  @Override
  public boolean isCompatible(Type other) {
    if (!(other instanceof Product)) {
      return false;
    }

    Product otherProd = (Product) other;
    if (types.size() != otherProd.types.size()) {
      return false;
    }

    for (int i = 0; i < types.size(); i++) {
      Type type = types.get(i);
      Type otherType = otherProd.types.get(i);
      if (!isCompatibleEitherWay(type, otherType)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Type construct(List<Type> others) {
    return this;
  }

  @Override
  public String usage() {
    if (types.isEmpty()) {
      return "(*)";
    }
    return types.stream().map(Type::usage).collect(Collectors.joining(", ", "(", ")"));
  }

  /** Perform two-way compatibility check here which is different from normal type expression */
  private boolean isCompatibleEitherWay(Type type1, Type type2) {
    return type1.isCompatible(type2) || type2.isCompatible(type1);
  }
}
