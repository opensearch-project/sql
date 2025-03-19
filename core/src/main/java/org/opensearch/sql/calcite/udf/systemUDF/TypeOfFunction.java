/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.systemUDF;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.udf.UserDefinedFunction;

public class TypeOfFunction implements UserDefinedFunction {

  @Override
  public Object eval(Object... args) {
    SqlTypeName sqlType = (SqlTypeName) args[0];
    switch (sqlType) {
      case BINARY:
      case VARBINARY:
        return "BINARY";
      case GEOMETRY:
        return "GEO_POINT";
      default:
        return convertSqlTypeNameToExprType(sqlType).legacyTypeName();
    }
  }
}
