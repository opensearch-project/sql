/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.HashSet;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.capability.KnnPluginCapability;

public class VectorSearchTableFunctionResolver implements FunctionResolver {

  public static final String VECTOR_SEARCH = "vectorsearch";
  public static final String TABLE = "table";
  public static final String FIELD = "field";
  public static final String VECTOR = "vector";
  public static final String OPTION = "option";
  public static final List<String> ARGUMENT_NAMES = List.of(TABLE, FIELD, VECTOR, OPTION);

  private final OpenSearchClient client;
  private final Settings settings;
  private final KnnPluginCapability knnCapability;

  public VectorSearchTableFunctionResolver(OpenSearchClient client, Settings settings) {
    this(client, settings, new KnnPluginCapability(client));
  }

  VectorSearchTableFunctionResolver(
      OpenSearchClient client, Settings settings, KnnPluginCapability knnCapability) {
    this.client = client;
    this.settings = settings;
    this.knnCapability = knnCapability;
  }

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature unresolvedSignature) {
    FunctionName functionName = FunctionName.of(VECTOR_SEARCH);
    FunctionSignature functionSignature =
        new FunctionSignature(functionName, List.of(STRING, STRING, STRING, STRING));
    FunctionBuilder functionBuilder =
        (functionProperties, arguments) -> {
          validateArguments(arguments);
          return new VectorSearchTableFunctionImplementation(
              functionName, arguments, client, settings, knnCapability);
        };
    return Pair.of(functionSignature, functionBuilder);
  }

  @Override
  public FunctionName getFunctionName() {
    return FunctionName.of(VECTOR_SEARCH);
  }

  private void validateArguments(List<Expression> arguments) {
    if (arguments.size() != ARGUMENT_NAMES.size()) {
      throw new ExpressionEvaluationException(
          String.format(
              "vectorSearch requires %d arguments (%s), got %d",
              ARGUMENT_NAMES.size(), String.join(", ", ARGUMENT_NAMES), arguments.size()));
    }
    // Shape check at the resolver so positional or unknown-named args produce a clean 400 before
    // planning proceeds. The Implementation layer repeats the non-named and duplicate-name checks
    // as defense-in-depth; the unknown-name allowlist is enforced only here because the
    // Implementation looks up values by known keys and does not need to re-validate the allowlist.
    HashSet<String> seen = new HashSet<>();
    for (Expression arg : arguments) {
      if (!(arg instanceof NamedArgumentExpression)) {
        throw new ExpressionEvaluationException(
            "vectorSearch() requires named arguments (e.g., table='index'), "
                + "but received: "
                + arg.getClass().getSimpleName());
      }
      String name = ((NamedArgumentExpression) arg).getArgName();
      if (name == null || name.isEmpty()) {
        throw new ExpressionEvaluationException(
            "vectorSearch() requires named arguments (e.g., table='index'), "
                + "but received an argument with no name");
      }
      String lower = name.toLowerCase(java.util.Locale.ROOT);
      if (!ARGUMENT_NAMES.contains(lower)) {
        throw new ExpressionEvaluationException(
            String.format(
                "Unknown argument name '%s' in vectorSearch(); allowed names are %s",
                name, ARGUMENT_NAMES));
      }
      if (!seen.add(lower)) {
        throw new ExpressionEvaluationException(
            "Duplicate argument name '"
                + name
                + "' in vectorSearch(); each named argument may appear at most once");
      }
    }
    // At this point `seen` holds exactly ARGUMENT_NAMES.size() entries (no duplicates, no unknowns,
    // and arity matches), so every required name is present. No separate missing-name check needed.
  }
}
