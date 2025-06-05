/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public class OpenSearchTypeSystem extends RelDataTypeSystemImpl {
  public static final RelDataTypeSystem INSTANCE = new OpenSearchTypeSystem();

  private OpenSearchTypeSystem() {}

  @Override
  public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
    if (SqlTypeName.DECIMAL == argumentType.getSqlTypeName()) {
      return typeFactory.createTypeWithNullability(highPrecision(typeFactory, argumentType), false);
    } else if (SqlTypeName.INT_TYPES.contains(argumentType.getSqlTypeName())) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.DOUBLE), false);
    } else {
      return argumentType;
    }
  }

  /**
   * Compute a higher precision version of a type.
   *
   * @return If type is a DECIMAL type, return a type with the precision and scale +4, to align the
   *     behaviour with Apache Spark.
   */
  public static RelDataType highPrecision(
      final RelDataTypeFactory typeFactory, final RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
      return typeFactory.createSqlType(
          type.getSqlTypeName(),
          Math.min(
              type.getPrecision() + 4,
              typeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL)),
          Math.min(
              type.getScale() + 4, typeFactory.getTypeSystem().getMaxScale(SqlTypeName.DECIMAL)));
    }
    return type;
  }
}
