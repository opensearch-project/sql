/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.SerializableCharset;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

public class ExprTimeStampSqlType extends CalciteBasicSqlUDT {

  public ExprTimeStampSqlType(RelDataTypeSystem typeSystem, boolean isNullable) {
    super(typeSystem, SqlTypeName.VARCHAR, isNullable);
  }

  public ExprTimeStampSqlType(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset) {
    super(typeSystem, typeName, nullable, precision, scale, collation, wrappedCharset);
  }

  @Override
  protected CalciteBasicSqlUDT createInstance(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset) {
    return new ExprTimeStampSqlType(
        typeSystem, typeName, nullable, precision, scale, collation, wrappedCharset);
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("expr_timestamp");
  }

  @Override
  public ExprType getExprType() {
    return ExprCoreType.TIMESTAMP;
  }
}
