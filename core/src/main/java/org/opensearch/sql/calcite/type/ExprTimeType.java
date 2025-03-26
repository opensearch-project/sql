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

public class ExprTimeType extends ExprBasicSqlUDT {

  public ExprTimeType(RelDataTypeSystem typeSystem, boolean isNullable) {
    super(typeSystem, SqlTypeName.VARCHAR, ExprUDT.EXPR_TIME, isNullable);
  }

  public ExprTimeType(
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
  protected ExprBasicSqlUDT createInstance(
      RelDataTypeSystem typeSystem,
      SqlTypeName typeName,
      ExprUDT udt,
      boolean nullable,
      int precision,
      int scale,
      @Nullable SqlCollation collation,
      @Nullable SerializableCharset wrappedCharset) {
    return new ExprTimeType(
        typeSystem, typeName, udt, nullable, precision, scale, collation, wrappedCharset);
  }
}
