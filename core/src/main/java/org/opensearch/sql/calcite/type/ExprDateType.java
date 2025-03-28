/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.SerializableCharset;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class ExprDateType extends ExprBasicSqlType {

  public ExprDateType(RelDataTypeSystem typeSystem, boolean isNullable) {
    super(
        typeSystem,
        SqlTypeName.VARCHAR,
        EXPR_DATE,
        isNullable,
        typeSystem.getDefaultPrecision(SqlTypeName.VARCHAR),
        typeSystem.getDefaultScale(SqlTypeName.VARCHAR),
        null,
        null);
  }

  public ExprDateType(
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
    return new ExprDateType(
        typeSystem, typeName, udt, nullable, precision, scale, collation, wrappedCharset);
  }
}
