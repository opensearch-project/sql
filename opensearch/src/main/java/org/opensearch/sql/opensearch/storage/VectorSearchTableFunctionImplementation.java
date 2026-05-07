/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.opensearch.storage.VectorSearchTableFunctionResolver.FIELD;
import static org.opensearch.sql.opensearch.storage.VectorSearchTableFunctionResolver.OPTION;
import static org.opensearch.sql.opensearch.storage.VectorSearchTableFunctionResolver.TABLE;
import static org.opensearch.sql.opensearch.storage.VectorSearchTableFunctionResolver.VECTOR;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.opensearch.sql.opensearch.storage.capability.KnnPluginCapability;
import org.opensearch.sql.storage.Table;

public class VectorSearchTableFunctionImplementation extends FunctionExpression
    implements TableFunctionImplementation {

  /**
   * P0 allowed option keys. Rejects unknown/future keys to prevent unvalidated DSL injection. A
   * {@link List} (rather than a {@link Set}) so the unknown-key error message renders the supported
   * keys in a stable, user-friendly order.
   */
  static final List<String> ALLOWED_OPTION_KEYS =
      List.of("k", "max_distance", "min_score", "filter_type");

  /**
   * Field names must be safe for JSON interpolation: alphanumeric, dots (nested), underscores,
   * hyphens. Rejects characters that could corrupt the WrapperQueryBuilder JSON. The same regex is
   * reused for table names so user-supplied identifiers cannot break out of the JSON context.
   */
  private static final Pattern SAFE_FIELD_NAME = Pattern.compile("^[a-zA-Z0-9._\\-]+$");

  private final FunctionName functionName;
  private final List<Expression> arguments;
  private final OpenSearchClient client;
  private final Settings settings;
  private final KnnPluginCapability knnCapability;

  public VectorSearchTableFunctionImplementation(
      FunctionName functionName,
      List<Expression> arguments,
      OpenSearchClient client,
      Settings settings,
      KnnPluginCapability knnCapability) {
    super(functionName, arguments);
    this.functionName = functionName;
    this.arguments = arguments;
    this.client = client;
    this.settings = settings;
    this.knnCapability = knnCapability;
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
    // Local validation runs first so that malformed queries return stable SQL validation errors
    // regardless of cluster state. The k-NN plugin presence is checked later, lazily at scan
    // open() time, so analysis-time paths (_explain, local validation) stay functional on
    // clusters without k-NN.
    validateNamedArgs();
    String tableName = getArgumentValue(TABLE);
    validateTableName(tableName);
    String fieldName = getArgumentValue(FIELD);
    validateFieldName(fieldName);
    String vectorLiteral = getArgumentValue(VECTOR);
    String optionStr = getArgumentValue(OPTION);

    float[] vector = parseVector(vectorLiteral);
    Map<String, String> options = parseOptions(optionStr);
    validateOptions(options);

    // Strip filter_type — it's a SQL-layer directive, not a knn parameter
    FilterType filterType = null;
    if (options.containsKey("filter_type")) {
      filterType = FilterType.fromString(options.remove("filter_type"));
    }

    return new VectorSearchIndex(
        client, settings, tableName, fieldName, vector, options, filterType, knnCapability);
  }

  private float[] parseVector(String vectorLiteral) {
    String cleaned = vectorLiteral.replaceAll("[\\[\\]]", "").trim();
    if (cleaned.isEmpty()) {
      throw new ExpressionEvaluationException("Vector literal must not be empty");
    }
    // Reject common non-comma separators before Float.parseFloat fails with a generic
    // "Invalid vector component" that doesn't hint the user at the separator.
    if (cleaned.indexOf(';') >= 0 || cleaned.indexOf(':') >= 0 || cleaned.indexOf('|') >= 0) {
      throw new ExpressionEvaluationException(
          String.format(
              "Invalid vector literal '%s': vector= requires comma-separated components,"
                  + " e.g., vector='[1.0,2.0,3.0]'",
              vectorLiteral));
    }
    // Preserve trailing empties (split(",", -1)) so malformed literals like "[1.0,]" or
    // "[1.0,,2.0]" surface an explicit error instead of silently shrinking the vector.
    String[] parts = cleaned.split(",", -1);
    float[] vector = new float[parts.length];
    for (int i = 0; i < parts.length; i++) {
      String component = parts[i].trim();
      if (component.isEmpty()) {
        throw new ExpressionEvaluationException(
            String.format(
                "Invalid vector component at position %d: must be a number (check for"
                    + " trailing or consecutive commas in '%s')",
                i, vectorLiteral));
      }
      try {
        vector[i] = Float.parseFloat(component);
      } catch (NumberFormatException e) {
        throw new ExpressionEvaluationException(
            String.format("Invalid vector component '%s': must be a number", component));
      }
      if (!Float.isFinite(vector[i])) {
        throw new ExpressionEvaluationException(
            String.format("Invalid vector component '%s': must be a finite number", component));
      }
    }
    return vector;
  }

  static Map<String, String> parseOptions(String optionStr) {
    Map<String, String> options = new LinkedHashMap<>();
    // A wholly empty option string is handled downstream with a clearer "missing required option"
    // message than a generic malformed-segment error.
    if (optionStr.trim().isEmpty()) {
      return options;
    }
    // split(",", -1) preserves trailing empties so malformed inputs like "k=5," or "k=5,,k2=v"
    // surface an explicit error instead of being silently dropped.
    String[] pairs = optionStr.split(",", -1);
    for (String pair : pairs) {
      String trimmed = pair.trim();
      if (trimmed.isEmpty()) {
        throw new ExpressionEvaluationException(
            "Malformed option segment '': expected key=value (check for trailing or"
                + " consecutive commas)");
      }
      String[] kv = trimmed.split("=", 2);
      if (kv.length != 2 || kv[0].trim().isEmpty() || kv[1].trim().isEmpty()) {
        throw new ExpressionEvaluationException(
            String.format("Malformed option segment '%s': expected key=value", trimmed));
      }
      String key = kv[0].trim();
      if (options.containsKey(key)) {
        throw new ExpressionEvaluationException(String.format("Duplicate option key '%s'", key));
      }
      options.put(key, kv[1].trim());
    }
    return options;
  }

  /**
   * Reject non-named arguments, null arg names, and duplicate named arguments early. Runs before
   * any list-index-based lookup so a malformed argument list can never cause an AIOOBE downstream.
   */
  private void validateNamedArgs() {
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
      if (!seen.add(name.toLowerCase(java.util.Locale.ROOT))) {
        throw new ExpressionEvaluationException(
            "Duplicate argument name '"
                + name
                + "' in vectorSearch(); each named argument may appear at most once");
      }
    }
  }

  /**
   * Reject table names with characters that could corrupt the WrapperQueryBuilder JSON or escape
   * the target index name. Allows alphanumeric, dots, underscores, and hyphens (the characters
   * OpenSearch index names already permit). Explicitly rejects wildcards ('*') and multi-target
   * patterns (comma-separated) with a dedicated message, because vectorSearch() targets a single
   * concrete index or alias and fan-out patterns would otherwise fall through to the generic regex
   * message. Also rejects the `_all` routing target and the pathologic `.` / `..` names because
   * those either fan out to every index or are not valid concrete index names. Other native-invalid
   * names (leading dot, leading hyphen, bare underscore, uppercase, and so on) are intentionally
   * passed through for the OpenSearch client to reject with its own error message.
   */
  private void validateTableName(String tableName) {
    // Dedicated error for fan-out patterns ('*' and ',') before the generic regex; see Javadoc
    // for why vectorSearch() targets a single index.
    if (tableName.indexOf('*') >= 0 || tableName.indexOf(',') >= 0) {
      throw new ExpressionEvaluationException(
          String.format(
              "Invalid table name '%s': vectorSearch() requires a single concrete index or alias;"
                  + " wildcards ('*') and multi-target patterns (comma-separated) are not"
                  + " supported",
              tableName));
    }
    if (!SAFE_FIELD_NAME.matcher(tableName).matches()) {
      throw new ExpressionEvaluationException(
          String.format(
              "Invalid table name '%s': must contain only alphanumeric characters,"
                  + " dots, underscores, or hyphens",
              tableName));
    }
    String lower = tableName.toLowerCase(java.util.Locale.ROOT);
    if (lower.equals("_all") || tableName.equals(".") || tableName.equals("..")) {
      throw new ExpressionEvaluationException(
          String.format(
              "Invalid table name '%s': vectorSearch() requires a single concrete index or alias;"
                  + " '_all', '.', and '..' are not supported",
              tableName));
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
    if (options.containsKey("filter_type")) {
      // Validate early — fromString throws if invalid
      FilterType.fromString(options.get("filter_type"));
    }
    boolean hasK = options.containsKey("k");
    boolean hasMaxDistance = options.containsKey("max_distance");
    boolean hasMinScore = options.containsKey("min_score");
    if (!hasK && !hasMaxDistance && !hasMinScore) {
      throw new ExpressionEvaluationException(
          "Missing required option: one of k, max_distance, or min_score");
    }
    // Mutual exclusivity: exactly one search mode allowed
    int modeCount = (hasK ? 1 : 0) + (hasMaxDistance ? 1 : 0) + (hasMinScore ? 1 : 0);
    if (modeCount > 1) {
      throw new ExpressionEvaluationException(
          "Only one of k, max_distance, or min_score may be specified");
    }
    // Parse and canonicalize numeric values — closes JSON injection via option values
    if (hasK) {
      int k = parseIntOption(options, "k");
      if (k < 1 || k > 10000) {
        throw new ExpressionEvaluationException(
            String.format("k must be between 1 and 10000, got %d", k));
      }
    }
    if (hasMaxDistance) {
      double maxDistance = parseDoubleOption(options, "max_distance");
      if (maxDistance < 0) {
        throw new ExpressionEvaluationException(
            String.format(
                "max_distance must be non-negative, got %s", options.get("max_distance")));
      }
    }
    if (hasMinScore) {
      double minScore = parseDoubleOption(options, "min_score");
      if (minScore < 0) {
        throw new ExpressionEvaluationException(
            String.format("min_score must be non-negative, got %s", options.get("min_score")));
      }
    }
  }

  private int parseIntOption(Map<String, String> options, String key) {
    try {
      int value = Integer.parseInt(options.get(key));
      options.put(key, Integer.toString(value));
      return value;
    } catch (NumberFormatException e) {
      throw new ExpressionEvaluationException(
          String.format("Option '%s' must be an integer, got '%s'", key, options.get(key)));
    }
  }

  private double parseDoubleOption(Map<String, String> options, String key) {
    try {
      double value = Double.parseDouble(options.get(key));
      if (!Double.isFinite(value)) {
        throw new ExpressionEvaluationException(
            String.format("Option '%s' must be a finite number, got '%s'", key, options.get(key)));
      }
      options.put(key, Double.toString(value));
      return value;
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
