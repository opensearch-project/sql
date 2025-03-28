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
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class ExprTimeStampType extends ExprBasicSqlType {

  public ExprTimeStampType(RelDataTypeSystem typeSystem, boolean isNullable) {
    super(
        typeSystem,
        SqlTypeName.VARCHAR,
        ExprUDT.EXPR_TIMESTAMP,
        isNullable,
        typeSystem.getDefaultPrecision(SqlTypeName.VARCHAR),
        typeSystem.getDefaultScale(SqlTypeName.VARCHAR),
        null,
        null);
  }

  public ExprTimeStampType(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      ExprUDT udt,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset) {
    super(typeSystem, typeName, udt, nullable, precision, scale, collation, wrappedCharset);
  }

  @Override
  protected ExprBasicSqlType createInstance(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      ExprUDT udt,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset) {
    return new ExprTimeStampType(
        typeSystem, typeName, udt, nullable, precision, scale, collation, wrappedCharset);
  }
}
