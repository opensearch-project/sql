/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Internal MAP_REMOVE function that removes specified keys from a map. Function signature:
 * map_remove(map, array_of_keys) -> map Used internally for dynamic fields implementation to dedupe
 * field names in _MAP.
 */
public class MapRemoveFunctionImpl extends ImplementorUDF {

  public MapRemoveFunctionImpl() {
    super(new MapRemoveImplementor(), NullPolicy.ARG0);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      // Return type is the same as the first argument (the map)
      RelDataType mapType = sqlOperatorBinding.getOperandType(0);
      return sqlOperatorBinding.getTypeFactory().createTypeWithNullability(mapType, true);
    };
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.ARRAY));
  }

  public static class MapRemoveImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      return Expressions.call(
          Types.lookupMethod(MapRemoveFunctionImpl.class, "mapRemove", Object.class, Object.class),
          translatedOperands.get(0),
          translatedOperands.get(1));
    }
  }

  /**
   * Removes specified keys from a map.
   *
   * @param mapArg the input map
   * @param keysArg the array/list of keys to remove
   * @return a new map with the specified keys removed, or null if input map is null
   */
  @SuppressWarnings("unchecked")
  public static Object mapRemove(Object mapArg, Object keysArg) {
    if (mapArg == null || keysArg == null) {
      return mapArg;
    }

    verifyArgTypes(mapArg, keysArg);

    return mapRemove((Map<String, Object>) mapArg, (List<Object>) keysArg);
  }

  private static void verifyArgTypes(Object mapArg, Object keysArg) {
    if (!(mapArg instanceof Map)) {
      throw new IllegalArgumentException("First argument must be a map, got: " + mapArg.getClass());
    }

    if (!(keysArg instanceof List)) {
      throw new IllegalArgumentException(
          "Second argument must be an array/list, got: " + keysArg.getClass());
    }
  }

  private static Map<String, Object> mapRemove(
      Map<String, Object> originalMap, List<Object> keysToRemove) {
    Map<String, Object> resultMap = new HashMap<>(originalMap);

    for (Object keyObj : keysToRemove) {
      if (keyObj != null) {
        String key = keyObj.toString();
        resultMap.remove(key);
      }
    }

    return resultMap;
  }
}
