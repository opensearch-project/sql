/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.function;

import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DOUBLE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.OPENSEARCH_TYPE;
import static org.opensearch.sql.legacy.antlr.semantic.types.special.Generic.T;

import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.TypeExpression;

/** Aggregate function */
public enum AggregateFunction implements TypeExpression {
  COUNT(
      func().to(INTEGER), // COUNT(*)
      func(OPENSEARCH_TYPE).to(INTEGER)),
  MAX(func(T(NUMBER)).to(T)),
  MIN(func(T(NUMBER)).to(T)),
  AVG(func(T(NUMBER)).to(DOUBLE)),
  SUM(func(T(NUMBER)).to(T));

  private TypeExpressionSpec[] specifications;

  AggregateFunction(TypeExpressionSpec... specifications) {
    this.specifications = specifications;
  }

  @Override
  public String getName() {
    return name();
  }

  @Override
  public TypeExpressionSpec[] specifications() {
    return specifications;
  }

  private static TypeExpressionSpec func(Type... argTypes) {
    return new TypeExpressionSpec().map(argTypes);
  }

  @Override
  public String toString() {
    return "Function [" + name() + "]";
  }
}
