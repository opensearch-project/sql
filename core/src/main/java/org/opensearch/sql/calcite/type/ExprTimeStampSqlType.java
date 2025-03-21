/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.charset.Charset;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.SerializableCharset;

public class ExprTimeStampSqlType extends BasicSqlType {

  public ExprTimeStampSqlType(RelDataTypeSystem typeSystem, boolean isNullable) {
    super(typeSystem, SqlTypeName.VARCHAR, isNullable);
  }

  @Override
  protected void generateTypeString(StringBuilder sb, boolean withDetail) {
    sb.append("expr_timestamp");
  }

  BasicSqlType createWithNullability(boolean nullable) {
    if (nullable == this.isNullable) {
      return this;
    }
    return new ExprTimeStampSqlType(this.typeSystem, nullable);
  }

  /**
   * Constructs a type with charset and collation.
   *
   * <p>This must be a character type.
   */
  BasicSqlType createWithCharsetAndCollation(Charset charset, SqlCollation collation) {
    checkArgument(SqlTypeUtil.inCharFamily(this));
    return new ExprTimeStampSqlType(this.typeSystem, this.isNullable);
  }
}
