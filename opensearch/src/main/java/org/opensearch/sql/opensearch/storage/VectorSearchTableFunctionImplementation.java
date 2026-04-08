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
import java.util.Set;
import java.util.regex.Pattern;
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

  /** P0 allowed option keys. Rejects unknown/future keys to prevent unvalidated DSL injection. */
  static final Set<String> ALLOWED_OPTION_KEYS = Set.of("k", "max_distance", "min_score");

  /**
   * Field names must be safe for JSON interpolation: alphanumeric, dots (nested), underscores,
   * hyphens. Rejects characters that could corrupt the WrapperQueryBuilder JSON.
   */
  private static final Pattern SAFE_FIELD_NAME = Pattern.compile("^[a-zA-Z0-9._\\-]+$");

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
                arg -> {
                  if (arg instanceof NamedArgumentExpression) {
                    NamedArgumentExpression named = (NamedArgumentExpression) arg;
                    return String.format("%s=%s", named.getArgName(), named.getValue().toString());
                  }
                  return arg.toString();
                })
            .collect(Collectors.toList());
    return String.format("%s(%s)", functionName, String.join(", ", args));
  }

  @Override
  public Table applyArguments() {
    validateNamedArgs();
    String tableName = getArgumentValue(TABLE);
    String fieldName = getArgumentValue(FIELD);
    validateFieldName(fieldName);
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

  /** Reject non-named arguments early. vectorSearch() requires named args (key=value). */
  private void validateNamedArgs() {
    for (Expression arg : arguments) {
      if (!(arg instanceof NamedArgumentExpression)) {
        throw new ExpressionEvaluationException(
            "vectorSearch() requires named arguments (e.g., table='index'), "
                + "but received: "
                + arg.getClass().getSimpleName());
      }
    }
  }

  /**
   * Reject field names with characters that could corrupt the WrapperQueryBuilder JSON. Allows
   * alphanumeric, dots (nested fields), underscores, and hyphens.
   */
  private void validateFieldName(String fieldName) {
    if (!SAFE_FIELD_NAME.matcher(fieldName).matches()) {
      throw new ExpressionEvaluationException(
          String.format(
              "Invalid field name '%s': must contain only alphanumeric characters,"
                  + " dots, underscores, or hyphens",
              fieldName));
    }
  }

  /**
   * Validates and canonicalizes option values. All P0 option values must be numeric. Parsing them
   * here prevents non-numeric strings from reaching the raw JSON construction in buildKnnQuery().
   */
  private void validateOptions(Map<String, String> options) {
    // Reject unknown option keys — only P0 keys are allowed
    for (String key : options.keySet()) {
      if (!ALLOWED_OPTION_KEYS.contains(key)) {
        throw new ExpressionEvaluationException(
            String.format("Unknown option key '%s'. Supported keys: %s", key, ALLOWED_OPTION_KEYS));
      }
    }
    boolean hasK = options.containsKey("k");
    boolean hasMaxDistance = options.containsKey("max_distance");
    boolean hasMinScore = options.containsKey("min_score");
    if (!hasK && !hasMaxDistance && !hasMinScore) {
      throw new ExpressionEvaluationException(
          "Missing required option: one of k, max_distance, or min_score");
    }
    // Parse and canonicalize numeric values — closes JSON injection via option values
    if (hasK) {
      parseIntOption(options, "k");
    }
    if (hasMaxDistance) {
      parseDoubleOption(options, "max_distance");
    }
    if (hasMinScore) {
      parseDoubleOption(options, "min_score");
    }
  }

  private void parseIntOption(Map<String, String> options, String key) {
    try {
      int value = Integer.parseInt(options.get(key));
      options.put(key, Integer.toString(value));
    } catch (NumberFormatException e) {
      throw new ExpressionEvaluationException(
          String.format("Option '%s' must be an integer, got '%s'", key, options.get(key)));
    }
  }

  private void parseDoubleOption(Map<String, String> options, String key) {
    try {
      double value = Double.parseDouble(options.get(key));
      if (!Double.isFinite(value)) {
        throw new ExpressionEvaluationException(
            String.format("Option '%s' must be a finite number, got '%s'", key, options.get(key)));
      }
      options.put(key, Double.toString(value));
    } catch (NumberFormatException e) {
      throw new ExpressionEvaluationException(
          String.format("Option '%s' must be a number, got '%s'", key, options.get(key)));
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
