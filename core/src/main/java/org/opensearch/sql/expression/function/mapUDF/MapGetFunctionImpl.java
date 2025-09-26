/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.mapUDF;

import static org.opensearch.sql.calcite.utils.PPLReturnTypes.STRING_FORCE_NULLABLE;

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
 * Implementation of MAP_GET function for accessing values from MAP<STRING, ANY> objects. This
 * function retrieves a value from a map using a string key.
 *
 * <p>Usage: MAP_GET(map, key) -> value
 */
public class MapGetFunctionImpl extends ImplementorUDF {

  public MapGetFunctionImpl() {
    super(new MapGetImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    // Value type is always String for simplicity right now.
    return STRING_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.MAP, SqlTypeFamily.STRING));
  }

  public static class MapGetImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      ScalarFunctionImpl function =
          (ScalarFunctionImpl)
              ScalarFunctionImpl.create(
                  Types.lookupMethod(MapGetFunctionImpl.class, "eval", Object[].class));
      return function.getImplementor().implement(translator, call, RexImpTable.NullAs.NULL);
    }
  }

  /**
   * Evaluates the MAP_GET function.
   *
   * @param args Function arguments: [map, key]
   * @return The value associated with the key, or null if not found
   */
  public static Object eval(Object... args) {
    if (args.length != 2) {
      throw new IllegalArgumentException(
          "MAP_GET function requires exactly 2 arguments: map and key");
    }

    if (args[0] == null || args[1] == null) {
      return null;
    }

    // Ensure the first argument is a Map
    if (!(args[0] instanceof Map)) {
      throw new IllegalArgumentException(
          "First argument to MAP_GET must be a Map, got: " + args[0].getClass().getSimpleName());
    }

    // Ensure the second argument is a String (key)
    if (!(args[1] instanceof String)) {
      throw new IllegalArgumentException(
          "Second argument to MAP_GET must be a String key, got: "
              + args[1].getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) args[0];
    String key = (String) args[1];

    return map.get(key);
  }
}
