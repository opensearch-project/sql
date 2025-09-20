/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Utility class for creating MAP access operations in Calcite. This class provides helper methods
 * for generating RexNode expressions that access MAP fields, supporting dynamic column
 * functionality.
 */
public class MapAccessOperations {

  /**
   * Creates a MAP access expression: MAP_GET(mapExpr, key)
   *
   * @param rexBuilder The RexBuilder to create expressions
   * @param mapExpr The MAP expression to access
   * @param key The key to access in the MAP
   * @return RexNode representing MAP_GET operation
   */
  public static RexNode mapGet(RexBuilder rexBuilder, RexNode mapExpr, String key) {
    RexNode keyLiteral = rexBuilder.makeLiteral(key);
    return rexBuilder.makeCall(PPLBuiltinOperators.MAP_GET, mapExpr, keyLiteral);
  }

  /**
   * Creates a MAP keys expression: MAP_KEYS(mapExpr) Note: MAP_KEYS is not available in current
   * Calcite version, so this method is not implemented. For dynamic columns, we rely on known field
   * names rather than runtime key discovery.
   *
   * @param rexBuilder The RexBuilder to create expressions
   * @param mapExpr The MAP expression to get keys from
   * @return RexNode representing MAP_KEYS operation
   */
  public static RexNode mapKeys(RexBuilder rexBuilder, RexNode mapExpr) {
    // MAP_KEYS is not available in Calcite 1.38.0
    // For dynamic columns implementation, we don't need runtime key discovery
    // since field names are known at query planning time
    throw new UnsupportedOperationException(
        "MAP_KEYS operation is not supported in current Calcite version. "
            + "Dynamic columns use known field names at planning time.");
  }

  /**
   * Creates a MAP contains key expression: mapExpr IS NOT NULL AND mapExpr[key] IS NOT NULL
   *
   * @param rexBuilder The RexBuilder to create expressions
   * @param mapExpr The MAP expression to check
   * @param key The key to check for existence
   * @return RexNode representing MAP contains key operation
   */
  public static RexNode mapContainsKey(RexBuilder rexBuilder, RexNode mapExpr, String key) {
    RexNode mapNotNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, mapExpr);
    RexNode mapAccess = mapGet(rexBuilder, mapExpr, key);
    RexNode valueNotNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, mapAccess);

    return rexBuilder.makeCall(SqlStdOperatorTable.AND, mapNotNull, valueNotNull);
  }

  /**
   * Creates a dynamic columns MAP field reference. This is the standard field name used for storing
   * dynamic columns.
   *
   * @return The field name for dynamic columns MAP
   */
  public static String getDynamicColumnsFieldName() {
    return DynamicColumnProcessor.DYNAMIC_COLUMNS_FIELD;
  }

  /**
   * Creates a MAP type for dynamic columns: MAP<STRING, ANY>
   *
   * @param typeFactory The type factory to create types
   * @return RelDataType representing MAP<STRING, ANY>
   */
  public static org.apache.calcite.rel.type.RelDataType createDynamicColumnsMapType(
      OpenSearchTypeFactory typeFactory) {
    org.apache.calcite.rel.type.RelDataType stringType =
        typeFactory.createSqlType(SqlTypeName.VARCHAR);
    org.apache.calcite.rel.type.RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
    return typeFactory.createMapType(stringType, anyType, true);
  }

  /**
   * Checks if a field name represents the dynamic columns MAP field.
   *
   * @param fieldName The field name to check
   * @return true if this is the dynamic columns field
   */
  public static boolean isDynamicColumnsField(String fieldName) {
    return getDynamicColumnsFieldName().equals(fieldName);
  }
}
