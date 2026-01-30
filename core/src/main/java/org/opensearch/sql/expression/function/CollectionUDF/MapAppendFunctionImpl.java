/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.CollectionUDF;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * MapAppend function that merges two maps. Value for the same key will be merged into an array by
 * using {@link AppendCore}.
 */
public class MapAppendFunctionImpl extends ImplementorUDF {

  public MapAppendFunctionImpl() {
    super(new MapAppendImplementor(), NullPolicy.ALL);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return sqlOperatorBinding -> {
      RelDataTypeFactory typeFactory = sqlOperatorBinding.getTypeFactory();
      return typeFactory.createMapType(
          typeFactory.createSqlType(SqlTypeName.VARCHAR),
          typeFactory.createSqlType(SqlTypeName.ANY));
    };
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.MAP));
  }

  public static class MapAppendImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      if (translatedOperands.size() != 2) {
        throw new IllegalArgumentException("MAP_APPEND function requires exactly 2 arguments");
      }

      return Expressions.call(
          Types.lookupMethod(MapAppendFunctionImpl.class, "mapAppend", Object.class, Object.class),
          translatedOperands.get(0),
          translatedOperands.get(1));
    }
  }

  public static Object mapAppend(Object map1, Object map2) {
    if (map1 == null && map2 == null) {
      return null;
    }
    if (map1 == null) {
      return mapAppendImpl(verifyMap(map2));
    }
    if (map2 == null) {
      return mapAppendImpl(verifyMap(map1));
    }

    return mapAppendImpl(verifyMap(map1), verifyMap(map2));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> verifyMap(Object map) {
    if (!(map instanceof Map)) {
      throw new IllegalArgumentException(
          "MAP_APPEND function requires both arguments to be MAP type");
    }
    return (Map<String, Object>) map;
  }

  static Map<String, Object> mapAppendImpl(Map<String, Object> map) {
    Map<String, Object> result = new HashMap<>();
    for (String key : map.keySet()) {
      result.put(key, AppendCore.collectElements(map.get(key)));
    }
    return result;
  }

  static Map<String, Object> mapAppendImpl(
      Map<String, Object> firstMap, Map<String, Object> secondMap) {
    Map<String, Object> result = new HashMap<>();

    for (String key : mergeKeys(firstMap, secondMap)) {
      result.put(key, AppendCore.collectElements(firstMap.get(key), secondMap.get(key)));
    }

    return result;
  }

  private static Set<String> mergeKeys(
      Map<String, Object> firstMap, Map<String, Object> secondMap) {
    Set<String> keys = new HashSet<>();
    keys.addAll(firstMap.keySet());
    keys.addAll(secondMap.keySet());
    return keys;
  }
}
