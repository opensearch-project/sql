/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.analysis.symbol.SymbolTable;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

/**
 * The definition of Type Environment.
 */
public class TypeEnvironment implements Environment<Symbol, ExprType> {
  @Getter
  private final TypeEnvironment parent;
  private final SymbolTable symbolTable;

  public TypeEnvironment(TypeEnvironment parent) {
    this.parent = parent;
    this.symbolTable = new SymbolTable();
  }

  public TypeEnvironment(TypeEnvironment parent, SymbolTable symbolTable) {
    this.parent = parent;
    this.symbolTable = symbolTable;
  }

  /**
   * Resolve the {@link Expression} from environment.
   *
   * @param symbol Symbol
   * @return resolved {@link ExprType}
   */
  @Override
  public ExprType resolve(Symbol symbol) {
    for (TypeEnvironment cur = this; cur != null; cur = cur.parent) {
      Optional<ExprType> typeOptional = cur.symbolTable.lookup(symbol);
      if (typeOptional.isPresent()) {
        return typeOptional.get();
      }
    }
    throw new SemanticCheckException(
        String.format("can't resolve %s in type env", symbol));
  }

  /**
   * Resolve all fields in the current environment.
   * @param namespace     a namespace
   * @return              all symbols in the namespace
   */
  public Map<String, ExprType> lookupAllFields(Namespace namespace) {
    Map<String, ExprType> result = new LinkedHashMap<>();
    symbolTable.lookupAllFields(namespace).forEach(result::putIfAbsent);
    return result;
  }

  /**
   * Define symbol with the type.
   *
   * @param symbol symbol to define
   * @param type   type
   */
  public void define(Symbol symbol, ExprType type) {
    symbolTable.store(symbol, type);
  }

  /**
   * Define expression with the type.
   *
   * @param ref {@link ReferenceExpression}
   */
  public void define(ReferenceExpression ref) {
    define(new Symbol(Namespace.FIELD_NAME, ref.getAttr()), ref.type());
  }

  public void remove(Symbol symbol) {
    symbolTable.remove(symbol);
  }

  /**
   * Remove ref.
   */
  public void remove(ReferenceExpression ref) {
    remove(new Symbol(Namespace.FIELD_NAME, ref.getAttr()));
  }
}
