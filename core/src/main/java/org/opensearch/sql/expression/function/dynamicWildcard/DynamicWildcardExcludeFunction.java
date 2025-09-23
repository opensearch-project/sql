/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.dynamicWildcard;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.calcite.utils.WildcardUtils;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Dynamic wildcard exclude function that excludes wildcard patterns from MAP keys at runtime. This
 * function is used internally by the fields command to support wildcard exclusion patterns with
 * dynamic columns.
 *
 * <p>Function signature: DYNAMIC_WILDCARD_EXCLUDE(map_field, patterns) -> MAP
 *
 * <p>- map_field: The MAP field containing dynamic columns - patterns: Either a single string
 * pattern or array of string patterns
 *
 * <p>Returns a MAP containing only the keys that match NONE of the provided patterns.
 */
public class DynamicWildcardExcludeFunction extends ImplementorUDF {

  public DynamicWildcardExcludeFunction() {
    super(new DynamicWildcardExcludeImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    // Return type should be a MAP to match the expected _dynamic_columns field type
    return opBinding ->
        opBinding
            .getTypeFactory()
            .createMapType(
                opBinding
                    .getTypeFactory()
                    .createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR),
                opBinding
                    .getTypeFactory()
                    .createSqlType(org.apache.calcite.sql.type.SqlTypeName.ANY));
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    // Always expect MAP and ARRAY - single patterns will be passed as single-element arrays
    return UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.ARRAY));
  }

  public static class DynamicWildcardExcludeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(DynamicWildcardExcludeFunction.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /**
   * Evaluates the DYNAMIC_WILDCARD_EXCLUDE function.
   *
   * @param args Function arguments: [map, patterns]
   * @return A MAP containing keys that match none of the patterns
   */
  public static Object eval(Object... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "DYNAMIC_WILDCARD_EXCLUDE function requires exactly 2 arguments: map and patterns");
    }

    Object mapObj = args[0];
    Object patternsObj = args[1];

    // Handle null inputs
    if (mapObj == null) {
      return new LinkedHashMap<>();
    }

    if (patternsObj == null) {
      // If no patterns to exclude, return the original map
      return mapObj;
    }

    // Ensure the first argument is a Map
    if (!(mapObj instanceof Map)) {
      throw new IllegalArgumentException(
          "First argument to DYNAMIC_WILDCARD_EXCLUDE must be a Map, got: "
              + mapObj.getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> sourceMap = (Map<String, Object>) mapObj;

    // Extract patterns from single string or array
    List<String> patterns = extractPatterns(patternsObj);
    if (patterns.isEmpty()) {
      // If no patterns to exclude, return the original map
      return new LinkedHashMap<>(sourceMap);
    }

    Map<String, Object> resultMap = new LinkedHashMap<>();

    // Single pass through the map, exclude if matches any pattern
    for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
      String key = entry.getKey();
      boolean shouldExclude = false;
      for (String pattern : patterns) {
        if (WildcardUtils.matchesWildcardPattern(pattern, key)) {
          shouldExclude = true;
          break; // Found match, exclude this key
        }
      }
      if (!shouldExclude) {
        resultMap.put(key, entry.getValue());
      }
    }

    return resultMap;
  }

  /** Extract patterns from single string or array value. */
  private static List<String> extractPatterns(Object patternsObj) {
    if (patternsObj instanceof String) {
      return List.of((String) patternsObj);
    } else if (patternsObj instanceof List) {
      @SuppressWarnings("unchecked")
      List<Object> patternsList = (List<Object>) patternsObj;
      return patternsList.stream().map(Object::toString).collect(Collectors.toList());
    } else if (patternsObj instanceof Object[]) {
      Object[] patternsArray = (Object[]) patternsObj;
      return List.of(patternsArray).stream().map(Object::toString).collect(Collectors.toList());
    } else {
      // Single pattern as other object type
      return List.of(patternsObj.toString());
    }
  }
}
