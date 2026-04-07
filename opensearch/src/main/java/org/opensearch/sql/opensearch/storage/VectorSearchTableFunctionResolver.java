/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

@RequiredArgsConstructor
public class VectorSearchTableFunctionResolver implements FunctionResolver {

  public static final String VECTOR_SEARCH = "vectorsearch";
  public static final String TABLE = "table";
  public static final String FIELD = "field";
  public static final String VECTOR = "vector";
  public static final String OPTION = "option";
  public static final List<String> ARGUMENT_NAMES = List.of(TABLE, FIELD, VECTOR, OPTION);

  private final OpenSearchClient client;
  private final Settings settings;

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    FunctionName functionName = FunctionName.of(VECTOR_SEARCH);
    FunctionSignature functionSignature =
        new FunctionSignature(functionName, List.of(STRING, STRING, STRING, STRING));
    FunctionBuilder functionBuilder =
        (functionProperties, arguments) -> {
          validateArguments(arguments);
          return new VectorSearchTableFunctionImplementation(
              functionName, arguments, client, settings);
        };
    return Pair.of(functionSignature, functionBuilder);
  }

  @Override
  public FunctionName getFunctionName() {
    return FunctionName.of(VECTOR_SEARCH);
  }

  private void validateArguments(List<Expression> arguments) {
    if (arguments.size() != ARGUMENT_NAMES.size()) {
      throw new IllegalArgumentException(
          String.format(
              "vectorSearch requires %d arguments (%s), got %d",
              ARGUMENT_NAMES.size(), String.join(", ", ARGUMENT_NAMES), arguments.size()));
    }
  }
}
