/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.dynamicWildcard;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * Dynamic wildcard expand function that expands wildcard patterns against MAP keys at runtime. This
 * function is used internally by the fields command to support wildcard patterns with dynamic
 * columns.
 *
 * <p>Function signature: DYNAMIC_WILDCARD_EXPAND(map_field, patterns) -> MAP
 *
 * <p>- map_field: The MAP field containing dynamic columns - patterns: Either a single string
 * pattern or array of string patterns
 *
 * <p>Returns a MAP containing only the keys that match ANY of the provided patterns.
 */
public class DynamicWildcardExpandFunction extends ImplementorUDF {

  public DynamicWildcardExpandFunction() {
    super(new DynamicWildcardExpandImplementor(), NullPolicy.ANY);
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

  public static class DynamicWildcardExpandImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(DynamicWildcardExpandFunction.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /**
   * Evaluates the DYNAMIC_WILDCARD_EXPAND function.
   *
   * @param args Function arguments: [map, patterns]
   * @return A MAP containing keys that match any of the patterns
   */
  public static Object eval(Object... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "DYNAMIC_WILDCARD_EXPAND function requires exactly 2 arguments: map and patterns");
    }

    if (args[0] == null || args[1] == null) {
      return new LinkedHashMap<>();
    }

    if (!(args[0] instanceof Map)) {
      throw new IllegalArgumentException(
          "First argument to DYNAMIC_WILDCARD_EXPAND must be a Map, got: "
              + args[0].getClass().getSimpleName());
    }

    if (!(args[1] instanceof List)) {
      throw new IllegalArgumentException(
          "Second argument to DYNAMIC_WILDCARD_EXPAND must be a List, got: "
              + args[1].getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> sourceMap = (Map<String, Object>) args[0];

    @SuppressWarnings("unchecked")
    List<String> patterns = (List<String>) args[1];

    Map<String, Object> resultMap = new LinkedHashMap<>();

    // Single pass through the map, check against all patterns
    for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
      String key = entry.getKey();
      for (String pattern : patterns) {
        if (WildcardUtils.matchesWildcardPattern(pattern, key)) {
          resultMap.put(key, entry.getValue());
          break; // Found match, no need to check other patterns
        }
      }
    }

    return resultMap;
  }
}
