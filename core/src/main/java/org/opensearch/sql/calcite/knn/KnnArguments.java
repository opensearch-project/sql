/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.knn;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Knn;

/**
 * The arguments of a k-NN step as written in the query, read the same way from both forms: the
 * {@code knn} command and the {@code knn(...)} predicate.
 *
 * @param field the {@code knn_vector} field
 * @param vector the query vector expression, {@code array(<numeric literals>)}
 * @param options the {@code name=value} options
 */
public record KnnArguments(Field field, UnresolvedExpression vector, List<Argument> options) {

  /** The arguments of the {@code knn} command. */
  public static KnnArguments of(Knn node) {
    return new KnnArguments(node.getField(), node.getVector(), node.getOptions());
  }

  /**
   * The arguments of a {@code knn(<field>, <vector>, <name>=<value>...)} call.
   *
   * @throws org.opensearch.sql.exception.SemanticCheckException if the field is not a field
   *     reference, the vector is missing, or an option is not {@code name=<literal>}
   */
  public static KnnArguments of(Function knn) {
    List<UnresolvedExpression> args = knn.getFuncArgs();
    if (args.size() < 2) {
      throw KnnErrors.wrongArguments(knn);
    }
    if (!(args.get(0) instanceof Field field)) {
      throw KnnErrors.notAVectorField(args.get(0).toString());
    }
    List<Argument> options = new ArrayList<>();
    for (UnresolvedExpression arg : args.subList(2, args.size())) {
      if (!(arg instanceof UnresolvedArgument named)) {
        throw KnnErrors.unnamedOption(arg);
      }
      if (!(named.getValue() instanceof Literal literal)) {
        throw KnnErrors.invalidOptionValue(named.getArgName(), named.getValue());
      }
      options.add(new Argument(named.getArgName(), literal));
    }
    return new KnnArguments(field, args.get(1), options);
  }

  /** The field name as written, e.g. {@code embedding} or {@code doc.embedding}. */
  public String fieldName() {
    return field.getField().toString();
  }

  /**
   * The elements of the query vector.
   *
   * @throws org.opensearch.sql.exception.SemanticCheckException if the vector is not a non-empty
   *     {@code array(...)} of numeric literals
   */
  public List<Number> vectorElements() {
    if (!(vector instanceof Function array)
        || !"array".equalsIgnoreCase(array.getFuncName())
        || array.getFuncArgs().isEmpty()) {
      throw KnnErrors.invalidVector(vector);
    }
    List<Number> elements = new ArrayList<>();
    for (UnresolvedExpression element : array.getFuncArgs()) {
      if (!(element instanceof Literal literal) || !(literal.getValue() instanceof Number n)) {
        throw KnnErrors.invalidVector(vector);
      }
      elements.add(n);
    }
    return elements;
  }

  /** The validated options. */
  public KnnOptions parsedOptions() {
    return KnnOptions.parse(options);
  }
}
