/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;

import java.nio.charset.Charset;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

@UtilityClass
public class ValidationUtils {
  /**
   * Sync the nullability, collation, etc. to the target type. Copied from {@link
   * org.apache.calcite.sql.validate.implicit.AbstractTypeCoercion}
   */
  public static RelDataType syncAttributes(
      RelDataTypeFactory factory, RelDataType fromType, RelDataType toType) {
    RelDataType syncedType = toType;
    if (fromType != null) {
      syncedType = factory.createTypeWithNullability(syncedType, fromType.isNullable());
      if (SqlTypeUtil.inCharOrBinaryFamilies(fromType)
          && SqlTypeUtil.inCharOrBinaryFamilies(toType)) {
        Charset charset = fromType.getCharset();
        if (charset != null && SqlTypeUtil.inCharFamily(syncedType)) {
          SqlCollation collation = getCollation(fromType);
          syncedType = factory.createTypeWithCharsetAndCollation(syncedType, charset, collation);
        }
      }
    }
    return syncedType;
  }

  /**
   * Creates a user-defined type with attributes (nullability, charset, collation) copied from
   * another type.
   *
   * @param factory the type factory used to create the UDT
   * @param fromType the source type to copy attributes from (nullability, charset, collation)
   * @param userDefinedType the user-defined type to create
   * @return a new RelDataType representing the UDT with attributes from fromType
   */
  public static RelDataType createUDTWithAttributes(
      RelDataTypeFactory factory,
      RelDataType fromType,
      OpenSearchTypeFactory.ExprUDT userDefinedType) {
    if (!(factory instanceof OpenSearchTypeFactory typeFactory)) {
      throw new IllegalArgumentException("factory must be an instance of OpenSearchTypeFactory");
    }
    RelDataType type = typeFactory.createUDT(userDefinedType);
    return syncAttributes(typeFactory, fromType, type);
  }

  public static RelDataType createUDTWithAttributes(
      RelDataTypeFactory factory, RelDataType fromType, SqlTypeName sqlTypeName) {
    return switch (sqlTypeName) {
      case SqlTypeName.DATE ->
          createUDTWithAttributes(factory, fromType, OpenSearchTypeFactory.ExprUDT.EXPR_DATE);
      case SqlTypeName.TIME ->
          createUDTWithAttributes(factory, fromType, OpenSearchTypeFactory.ExprUDT.EXPR_TIME);
      case SqlTypeName.TIMESTAMP ->
          createUDTWithAttributes(factory, fromType, OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP);
      case SqlTypeName.BINARY ->
          createUDTWithAttributes(factory, fromType, OpenSearchTypeFactory.ExprUDT.EXPR_BINARY);
      default -> throw new IllegalArgumentException("Unsupported type: " + sqlTypeName);
    };
  }
}
