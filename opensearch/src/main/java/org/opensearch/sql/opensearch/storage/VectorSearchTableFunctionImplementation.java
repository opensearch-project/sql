/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.opensearch.storage.VectorSearchTableFunctionResolver.FIELD;
import static org.opensearch.sql.opensearch.storage.VectorSearchTableFunctionResolver.OPTION;
import static org.opensearch.sql.opensearch.storage.VectorSearchTableFunctionResolver.TABLE;
import static org.opensearch.sql.opensearch.storage.VectorSearchTableFunctionResolver.VECTOR;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.storage.Table;

public class VectorSearchTableFunctionImplementation extends FunctionExpression
    implements TableFunctionImplementation {

  private final FunctionName functionName;
  private final List<Expression> arguments;
  private final OpenSearchClient client;
  private final Settings settings;

  public VectorSearchTableFunctionImplementation(
      FunctionName functionName,
      List<Expression> arguments,
      OpenSearchClient client,
      Settings settings) {
    super(functionName, arguments);
    this.functionName = functionName;
    this.arguments = arguments;
    this.client = client;
    this.settings = settings;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new UnsupportedOperationException(
        String.format("vectorSearch function [%s] is only supported in FROM clause", functionName));
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRUCT;
  }

  @Override
  public String toString() {
    List<String> args =
        arguments.stream()
            .map(
                arg ->
                    String.format(
                        "%s=%s",
                        ((NamedArgumentExpression) arg).getArgName(),
                        ((NamedArgumentExpression) arg).getValue().toString()))
            .collect(Collectors.toList());
    return String.format("%s(%s)", functionName, String.join(", ", args));
  }

  @Override
  public Table applyArguments() {
    String tableName = getArgumentValue(TABLE);
    String fieldName = getArgumentValue(FIELD);
    String vectorLiteral = getArgumentValue(VECTOR);
    String optionStr = getArgumentValue(OPTION);

    float[] vector = parseVector(vectorLiteral);
    Map<String, String> options = parseOptions(optionStr);
    validateOptions(options);

    return new VectorSearchIndex(client, settings, tableName, fieldName, vector, options);
  }

  private float[] parseVector(String vectorLiteral) {
    String cleaned = vectorLiteral.replaceAll("[\\[\\]]", "").trim();
    String[] parts = cleaned.split(",");
    float[] vector = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      vector[i] = Float.parseFloat(parts[i].trim());
    }
    return vector;
  }

  static Map<String, String> parseOptions(String optionStr) {
    Map<String, String> options = new LinkedHashMap<>();
    for (String pair : optionStr.split(",")) {
      String[] kv = pair.trim().split("=", 2);
      if (kv.length == 2) {
        options.put(kv[0].trim(), kv[1].trim());
      }
    }
    return options;
  }

  private void validateOptions(Map<String, String> options) {
    boolean hasK = options.containsKey("k");
    boolean hasMaxDistance = options.containsKey("max_distance");
    boolean hasMinScore = options.containsKey("min_score");
    if (!hasK && !hasMaxDistance && !hasMinScore) {
      throw new ExpressionEvaluationException(
          "Missing required option: one of k, max_distance, or min_score");
    }
  }

  private String getArgumentValue(String name) {
    return arguments.stream()
        .filter(arg -> ((NamedArgumentExpression) arg).getArgName().equalsIgnoreCase(name))
        .map(arg -> ((NamedArgumentExpression) arg).getValue().valueOf().stringValue())
        .findFirst()
        .orElseThrow(
            () ->
                new ExpressionEvaluationException(
                    String.format("Missing required argument: %s", name)));
  }
}
