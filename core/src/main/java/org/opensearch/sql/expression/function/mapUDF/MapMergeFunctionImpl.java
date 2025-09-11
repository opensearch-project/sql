/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.mapUDF;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.MAP_STRING_ANY_FORCE_NULLABLE;

import java.util.HashMap;
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
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * Implementation of the map_merge function that merges two MAP objects. If both maps contain the
 * same key, the value from the second map takes precedence.
 */
public class MapMergeFunctionImpl extends ImplementorUDF {

  public MapMergeFunctionImpl() {
    super(new MapMergeImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return MAP_STRING_ANY_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.MAP));
  }

  public static class MapMergeImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(MapMergeFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /**
   * Evaluates the map_merge function with the given arguments.
   *
   * @param args Array of arguments where args[0] and args[1] are the maps to merge
   * @return Merged map, or null if both inputs are null
   */
  public static Object eval(Object... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("map_merge function requires exactly 2 arguments");
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> map1 = (Map<String, Object>) args[0];
    @SuppressWarnings("unchecked")
    Map<String, Object> map2 = (Map<String, Object>) args[1];

    return mergeMapValues(map1, map2);
  }

  /**
   * Merges two maps, with values from the second map taking precedence over the first.
   *
   * @param map1 First map (can be null)
   * @param map2 Second map (can be null)
   * @return Merged map, or null if both inputs are null
   */
  private static Map<String, Object> mergeMapValues(
      Map<String, Object> map1, Map<String, Object> map2) {
    if (map1 == null && map2 == null) {
      return null;
    }

    Map<String, Object> result = new HashMap<>();

    if (map1 != null) {
      result.putAll(map1);
    }

    if (map2 != null) {
      result.putAll(map2); // Values from map2 override map1
    }

    return result;
  }
}
