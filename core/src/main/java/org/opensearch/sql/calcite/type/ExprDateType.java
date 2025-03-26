package org.opensearch.sql.calcite.type;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.SerializableCharset;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class ExprDateType extends ExprBasicSqlUDT {

  public ExprDateType(RelDataTypeSystem typeSystem, boolean isNullable) {
    super(typeSystem, SqlTypeName.VARCHAR, EXPR_DATE, isNullable);
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
  protected ExprBasicSqlUDT createInstance(
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
