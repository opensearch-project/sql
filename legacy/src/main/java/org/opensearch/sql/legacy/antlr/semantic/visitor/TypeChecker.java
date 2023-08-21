/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.visitor;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.UNKNOWN;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.opensearch.sql.legacy.antlr.SimilarSymbols;
import org.opensearch.sql.legacy.antlr.semantic.SemanticAnalysisException;
import org.opensearch.sql.legacy.antlr.semantic.scope.Environment;
import org.opensearch.sql.legacy.antlr.semantic.scope.Namespace;
import org.opensearch.sql.legacy.antlr.semantic.scope.SemanticContext;
import org.opensearch.sql.legacy.antlr.semantic.scope.Symbol;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.TypeExpression;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType;
import org.opensearch.sql.legacy.antlr.semantic.types.function.AggregateFunction;
import org.opensearch.sql.legacy.antlr.semantic.types.function.OpenSearchScalarFunction;
import org.opensearch.sql.legacy.antlr.semantic.types.function.ScalarFunction;
import org.opensearch.sql.legacy.antlr.semantic.types.operator.ComparisonOperator;
import org.opensearch.sql.legacy.antlr.semantic.types.operator.JoinOperator;
import org.opensearch.sql.legacy.antlr.semantic.types.operator.SetOperator;
import org.opensearch.sql.legacy.antlr.semantic.types.special.Product;
import org.opensearch.sql.legacy.antlr.visitor.GenericSqlParseTreeVisitor;
import org.opensearch.sql.legacy.utils.StringUtils;

/** SQL semantic analyzer that determines if a syntactical correct query is meaningful. */
public class TypeChecker implements GenericSqlParseTreeVisitor<Type> {

  private static final Type NULL_TYPE =
      new Type() {
        @Override
        public String getName() {
          return "NULL";
        }

        @Override
        public boolean isCompatible(Type other) {
          throw new IllegalStateException("Compatibility check on NULL type with " + other);
        }

        @Override
        public Type construct(List<Type> others) {
          throw new IllegalStateException("Construct operation on NULL type with " + others);
        }

        @Override
        public String usage() {
          throw new IllegalStateException("Usage print operation on NULL type");
        }
      };

  /** Semantic context for symbol scope management */
  private final SemanticContext context;

  /** Should suggestion provided. Disabled by default for security concern. */
  private final boolean isSuggestEnabled;

  public TypeChecker(SemanticContext context) {
    this.context = context;
    this.isSuggestEnabled = false;
  }

  public TypeChecker(SemanticContext context, boolean isSuggestEnabled) {
    this.context = context;
    this.isSuggestEnabled = isSuggestEnabled;
  }

  @Override
  public void visitRoot() {
    defineFunctionNames(ScalarFunction.values());
    defineFunctionNames(OpenSearchScalarFunction.values());
    defineFunctionNames(AggregateFunction.values());
    defineOperatorNames(ComparisonOperator.values());
    defineOperatorNames(SetOperator.values());
    defineOperatorNames(JoinOperator.values());
  }

  @Override
  public void visitQuery() {
    context.push();
  }

  @Override
  public void endVisitQuery() {
    context.pop();
  }

  @Override
  public Type visitSelect(List<Type> itemTypes) {
    if (itemTypes.size() == 1) {
      return itemTypes.get(0);
    } else if (itemTypes.size() == 0) {
      return visitSelectAllColumn();
    }
    // Return product for empty (SELECT *) and #items > 1
    return new Product(itemTypes);
  }

  @Override
  public Type visitSelectAllColumn() {
    return resolveAllColumn();
  }

  @Override
  public void visitAs(String alias, Type type) {
    defineFieldName(alias, type);
  }

  @Override
  public Type visitIndexName(String indexName) {
    return resolve(new Symbol(Namespace.FIELD_NAME, indexName));
  }

  @Override
  public Type visitFieldName(String fieldName) {
    // Bypass hidden fields which is not present in mapping, ex. _id, _type.
    if (fieldName.startsWith("_")) {
      return UNKNOWN;
    }
    // Ignore case for function/operator though field name is case sensitive
    return resolve(new Symbol(Namespace.FIELD_NAME, fieldName));
  }

  @Override
  public Type visitFunctionName(String funcName) {
    return resolve(new Symbol(Namespace.FUNCTION_NAME, StringUtils.toUpper(funcName)));
  }

  @Override
  public Type visitOperator(String opName) {
    return resolve(new Symbol(Namespace.OPERATOR_NAME, StringUtils.toUpper(opName)));
  }

  @Override
  public Type visitString(String text) {
    return OpenSearchDataType.STRING;
  }

  @Override
  public Type visitInteger(String text) {
    return OpenSearchDataType.INTEGER;
  }

  @Override
  public Type visitFloat(String text) {
    return OpenSearchDataType.FLOAT;
  }

  @Override
  public Type visitBoolean(String text) {
    // "IS [NOT] MISSING" can be used on any data type
    return "MISSING".equalsIgnoreCase(text) ? UNKNOWN : OpenSearchDataType.BOOLEAN;
  }

  @Override
  public Type visitDate(String text) {
    return OpenSearchDataType.DATE;
  }

  @Override
  public Type visitNull() {
    return UNKNOWN;
  }

  @Override
  public Type visitConvertedType(String text) {
    return OpenSearchDataType.typeOf(text);
  }

  @Override
  public Type defaultValue() {
    return NULL_TYPE;
  }

  private void defineFieldName(String fieldName, Type type) {
    Symbol symbol = new Symbol(Namespace.FIELD_NAME, fieldName);
    if (!environment().resolve(symbol).isPresent()) {
      environment().define(symbol, type);
    }
  }

  private void defineFunctionNames(TypeExpression[] expressions) {
    for (TypeExpression expr : expressions) {
      environment().define(new Symbol(Namespace.FUNCTION_NAME, expr.getName()), expr);
    }
  }

  private void defineOperatorNames(Type[] expressions) {
    for (Type expr : expressions) {
      environment().define(new Symbol(Namespace.OPERATOR_NAME, expr.getName()), expr);
    }
  }

  private Type resolve(Symbol symbol) {
    Optional<Type> type = environment().resolve(symbol);
    if (type.isPresent()) {
      return type.get();
    }

    String errorMsg = StringUtils.format("%s cannot be found or used here.", symbol);

    if (isSuggestEnabled || symbol.getNamespace() != Namespace.FIELD_NAME) {
      Set<String> allSymbolsInScope = environment().resolveAll(symbol.getNamespace()).keySet();
      String suggestedWord = new SimilarSymbols(allSymbolsInScope).mostSimilarTo(symbol.getName());
      errorMsg += StringUtils.format(" Did you mean [%s]?", suggestedWord);
    }
    throw new SemanticAnalysisException(errorMsg);
  }

  private Type resolveAllColumn() {
    environment().resolveAll(Namespace.FIELD_NAME);
    return new Product(ImmutableList.of());
  }

  private Environment environment() {
    return context.peek();
  }
}
