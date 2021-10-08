/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.expression.function;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.stream.Collectors;
import lombok.ToString;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;

@UtilityClass
public class OpenSearchFunctions {
  public void register(BuiltinFunctionRepository repository) {
    repository.register(match());
  }

  private static FunctionResolver match() {
    FunctionName funcName = BuiltinFunctionName.MATCH.getName();
    return new FunctionResolver(funcName,
        ImmutableMap.<FunctionSignature, FunctionBuilder>builder()
            .put(new FunctionSignature(funcName, ImmutableList.of(STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList.of(STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList.of(STRING, STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .put(new FunctionSignature(funcName, ImmutableList
                    .of(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING,
                        STRING, STRING, STRING, STRING, STRING)),
                args -> new OpenSearchFunction(funcName, args))
            .build());
  }

  private static class OpenSearchFunction extends FunctionExpression {
    private final FunctionName functionName;
    private final List<Expression> arguments;

    public OpenSearchFunction(FunctionName functionName, List<Expression> arguments) {
      super(functionName, arguments);
      this.functionName = functionName;
      this.arguments = arguments;
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
      throw new UnsupportedOperationException(String.format(
          "OpenSearch defined function [%s] is only supported in WHERE and HAVING clause.",
          functionName));
    }

    @Override
    public ExprType type() {
      return ExprCoreType.BOOLEAN;
    }

    @Override
    public String toString() {
      List<String> args = arguments.stream()
          .map(arg -> String.format("%s=%s", ((NamedArgumentExpression) arg)
              .getArgName(), ((NamedArgumentExpression) arg).getValue().toString()))
          .collect(Collectors.toList());
      return String.format("%s(%s)", functionName, String.join(", ", args));
    }
  }
}
