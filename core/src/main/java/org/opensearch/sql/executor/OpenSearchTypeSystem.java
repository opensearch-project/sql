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
    if (SqlTypeName.INT_TYPES.contains(argumentType.getSqlTypeName())) {
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.DOUBLE), false);
    } else {
      return argumentType;
    }
  }

  @Override
  public int getMaxPrecision(SqlTypeName typeName) {
    switch (typeName) {
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIME_TZ:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_TZ:
        return 9;
      default:
        return super.getMaxPrecision(typeName);
    }
  }

}
