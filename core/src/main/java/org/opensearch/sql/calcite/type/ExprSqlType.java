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

/** The SqlType for ExprUDT. The UDT which uses a standard SQL type should extend this. */
public class ExprSqlType extends AbstractExprRelDataType<BasicSqlType> {
  public ExprSqlType(OpenSearchTypeFactory typeFactory, ExprUDT exprUDT, SqlTypeName sqlTypeName) {
    this(exprUDT, (BasicSqlType) typeFactory.createSqlType(sqlTypeName));
  }

  protected ExprSqlType(ExprUDT exprUDT, BasicSqlType type) {
    super(exprUDT, type);
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append(udt.name());
    sb.append(' ');
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
      Class<?> result;
      switch (type) {
          case VARCHAR:
          case CHAR:
              result = String.class;
              break;
          case DATE:
          case TIME:
          case TIME_WITH_LOCAL_TIME_ZONE:
          case TIME_TZ:
          case INTEGER:
          case INTERVAL_YEAR:
          case INTERVAL_YEAR_MONTH:
          case INTERVAL_MONTH:
              result = this.isNullable() ? Integer.class : int.class;
              break;
          case TIMESTAMP:
          case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          case TIMESTAMP_TZ:
          case BIGINT:
          case INTERVAL_DAY:
          case INTERVAL_DAY_HOUR:
          case INTERVAL_DAY_MINUTE:
          case INTERVAL_DAY_SECOND:
          case INTERVAL_HOUR:
          case INTERVAL_HOUR_MINUTE:
          case INTERVAL_HOUR_SECOND:
          case INTERVAL_MINUTE:
          case INTERVAL_MINUTE_SECOND:
          case INTERVAL_SECOND:
              result = this.isNullable() ? Long.class : long.class;
              break;
          case SMALLINT:
              result = this.isNullable() ? Short.class : short.class;
              break;
          case TINYINT:
              result = this.isNullable() ? Byte.class : byte.class;
              break;
          case DECIMAL:
              result = BigDecimal.class;
              break;
          case BOOLEAN:
              result = this.isNullable() ? Boolean.class : boolean.class;
              break;
          case DOUBLE:
          case FLOAT:
              result = this.isNullable() ? Double.class : double.class;
              break;
          case REAL:
              result = this.isNullable() ? Float.class : float.class;
              break;
          case BINARY:
          case VARBINARY:
              result = ByteString.class;
              break;
          case GEOMETRY:
              result = Geometry.class;
              break;
          case SYMBOL:
              result = Enum.class;
              break;
          case ANY:
              result = Object.class;
              break;
          case NULL:
              result = Void.class;
              break;
          default:
              throw new IllegalArgumentException("Unsupported sql type name: " + type);
      }
      return result;

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
