/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.system;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;

@UtilityClass
public class SystemFunctions {
  /** Register TypeOf Operator. */
  public static void register(BuiltinFunctionRepository repository) {
    repository.register(typeof());
  }

  // Auxiliary function useful for debugging
  private static FunctionResolver typeof() {
    return new FunctionResolver() {
      @Override
      public Pair<FunctionSignature, FunctionBuilder> resolve(
          FunctionSignature unresolvedSignature) {
        return Pair.of(
            unresolvedSignature,
            (functionProperties, arguments) ->
                new FunctionExpression(BuiltinFunctionName.TYPEOF.getName(), arguments) {
                  @Override
                  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
                    return new ExprStringValue(getArguments().get(0).type().legacyTypeName());
                  }

                  @Override
                  public ExprType type() {
                    return STRING;
                  }
                });
      }

      @Override
      public FunctionName getFunctionName() {
        return BuiltinFunctionName.TYPEOF.getName();
      }
    };
  }
}
