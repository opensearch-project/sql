/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

public class NestedFunctionResolver implements FunctionResolver{
  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    return Pair.of(unresolvedSignature,
        (functionProperties, arguments) ->
            new FunctionExpression(BuiltinFunctionName.NESTED.getName(), arguments) {
              @Override
              public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                return valueEnv.resolve(getArguments().get(0));
              }
              @Override
              public ExprType type() {
                return getArguments().get(0).type();
              }
            });
  }

  @Override
  public FunctionName getFunctionName() {
    return BuiltinFunctionName.NESTED.getName();
  }
}
