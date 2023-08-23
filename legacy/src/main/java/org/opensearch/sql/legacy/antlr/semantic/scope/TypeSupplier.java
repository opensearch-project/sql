/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.scope;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.opensearch.sql.legacy.antlr.semantic.SemanticAnalysisException;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;

/**
 * The TypeSupplier is construct by the symbolName and symbolType. The TypeSupplier implement the
 * {@link Supplier<Type>} interface to provide the {@link Type}. The TypeSupplier maintain types to
 * track different {@link Type} definition for the same symbolName.
 */
public class TypeSupplier implements Supplier<Type> {
  private final String symbolName;
  private final Type symbolType;
  private final Set<Type> types;

  public TypeSupplier(String symbolName, Type symbolType) {
    this.symbolName = symbolName;
    this.symbolType = symbolType;
    this.types = new HashSet<>();
    this.types.add(symbolType);
  }

  public TypeSupplier add(Type type) {
    types.add(type);
    return this;
  }

  /**
   * Get the {@link Type} Throw {@link SemanticAnalysisException} if conflict found. Currently, if
   * the two types not equal, they are treated as conflicting.
   */
  @Override
  public Type get() {
    if (types.size() > 1) {
      throw new SemanticAnalysisException(
          String.format("Field [%s] have conflict type [%s]", symbolName, types));
    } else {
      return symbolType;
    }
  }
}
