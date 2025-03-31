/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.locationtech.jts.geom.Geometry;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class ExprSqlType extends ExprRelDataType<BasicSqlType> {
  public ExprSqlType(OpenSearchTypeFactory typeFactory, ExprUDT exprUDT, SqlTypeName sqlTypeName) {
    this(exprUDT, (BasicSqlType) typeFactory.createSqlType(sqlTypeName));
  }

  protected ExprSqlType(ExprUDT exprUDT, BasicSqlType type) {
    super(exprUDT, type);
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(udt.name());
    try {
      Method method =
          BasicSqlType.class.getDeclaredMethod(
              "generateTypeString", StringBuilder.class, boolean.class);
      method.setAccessible(true);
      method.invoke(super.relType, sb, withDetail);
    } catch (Exception e) {
      throw new IllegalStateException("Unable to invoke generateTypeString for BasicSqlType", e);
    }
  }

  @Override
  public Type getJavaType() {
    SqlTypeName type = super.relType.getSqlTypeName();
    return switch (type) {
      case VARCHAR, CHAR -> String.class;
      case DATE,
          TIME,
          TIME_WITH_LOCAL_TIME_ZONE,
          TIME_TZ,
          INTEGER,
          INTERVAL_YEAR,
          INTERVAL_YEAR_MONTH,
          INTERVAL_MONTH -> this.isNullable() ? Integer.class : int.class;
      case TIMESTAMP,
          TIMESTAMP_WITH_LOCAL_TIME_ZONE,
          TIMESTAMP_TZ,
          BIGINT,
          INTERVAL_DAY,
          INTERVAL_DAY_HOUR,
          INTERVAL_DAY_MINUTE,
          INTERVAL_DAY_SECOND,
          INTERVAL_HOUR,
          INTERVAL_HOUR_MINUTE,
          INTERVAL_HOUR_SECOND,
          INTERVAL_MINUTE,
          INTERVAL_MINUTE_SECOND,
          INTERVAL_SECOND -> this.isNullable() ? Long.class : long.class;
      case SMALLINT -> this.isNullable() ? Short.class : short.class;
      case TINYINT -> this.isNullable() ? Byte.class : byte.class;
      case DECIMAL -> BigDecimal.class;
      case BOOLEAN -> this.isNullable() ? Boolean.class : boolean.class;
      case DOUBLE, FLOAT -> // sic
      this.isNullable() ? Double.class : double.class;
      case REAL -> this.isNullable() ? Float.class : float.class;
      case BINARY, VARBINARY -> ByteString.class;
      case GEOMETRY -> Geometry.class;
      case SYMBOL -> Enum.class;
      case ANY -> Object.class;
      case NULL -> Void.class;
      default -> throw new IllegalArgumentException("Unsupported sql type name: " + type);
    };
  }

  @Override
  public ExprSqlType createWithNullability(OpenSearchTypeFactory typeFactory, boolean nullable) {
    if (isNullable() == nullable) {
      return this;
    }
    return new ExprSqlType(
        super.udt, (BasicSqlType) typeFactory.createTypeWithNullability(super.relType, nullable));
  }

  @Override
  public ExprSqlType createWithCharsetAndCollation(
      OpenSearchTypeFactory typeFactory, Charset charset, SqlCollation collation) {
    return new ExprSqlType(
        super.udt,
        (BasicSqlType)
            typeFactory.createTypeWithCharsetAndCollation(super.relType, charset, collation));
  }
}
