/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import java.nio.charset.Charset;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.NonNullableAccessors;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

@UtilityClass
public class ValidationUtils {
  /**
   * Copy nullability, and for character or binary types also charset and collation, from a source
   * type onto a target type.
   *
   * @param factory the type factory used to create adjusted types
   * @param fromType the source type whose attributes (nullability, charset, collation) will be copied;
   *                 may be null, in which case the target type is returned unchanged
   * @param toType the target type to receive attributes from {@code fromType}
   * @return the resulting {@link RelDataType} based on {@code toType} with attributes synchronized
   *         from {@code fromType}
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
          SqlCollation collation = NonNullableAccessors.getCollation(fromType);
          syncedType = factory.createTypeWithCharsetAndCollation(syncedType, charset, collation);
        }
      }
    }
    return syncedType;
  }

  /**
   * Create a user-defined RelDataType and copy nullability, charset, and collation from another type.
   *
   * @param factory the type factory used to create the UDT; must be an instance of OpenSearchTypeFactory
   * @param fromType the source type whose nullability, charset, and collation will be copied (may be null)
   * @param userDefinedType the expression-defined UDT to create
   * @return the created UDT with attributes copied from {@code fromType}
   * @throws IllegalArgumentException if {@code factory} is not an instance of OpenSearchTypeFactory
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

  /**
   * Creates a user-defined type by mapping a SQL type name to the corresponding UDT, with
   * attributes copied from another type.
   *
   * @param factory the type factory used to create the UDT
   * @param fromType the source type to copy attributes from
   * @param sqlTypeName the SQL type name to map to a UDT (DATE, TIME, TIMESTAMP, or BINARY)
   * @return a new RelDataType representing the UDT with attributes from fromType
   * @throws IllegalArgumentException if the sqlTypeName is not supported
   */
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

  /**
   * Determines whether an exception matches known Calcite v1.41 validation errors related to
   * nested window functions and window functions used as function operands.
   *
   * <p>Used to tolerate specific Calcite validation failures (for example, incorrect detection of
   * window functions inside CASE expressions or failure to push projections containing OVER into
   * subqueries) that are treated as non-fatal/workaroundable within this codebase.
   *
   * @param e the exception whose message will be inspected for known Calcite error patterns
   * @return {@code true} if the exception's message contains a known Calcite validation error, {@code false} otherwise
   */
  public static boolean tolerantValidationException(Exception e) {
    List<String> acceptableErrorMessages =
        List.of(
            "Aggregate expressions cannot be nested",
            "Windowed aggregate expression is illegal in GROUP BY clause");
    return e.getMessage() != null
        && acceptableErrorMessages.stream().anyMatch(e.getMessage()::contains);
  }
}