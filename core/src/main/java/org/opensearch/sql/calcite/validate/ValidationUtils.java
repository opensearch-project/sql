/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import java.nio.charset.Charset;
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
          SqlCollation collation = NonNullableAccessors.getCollation(fromType);
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
   * Special handling for nested window functions that fail validation due to a Calcite bug.
   *
   * <p>This method provides a workaround for a known issue in Calcite v1.41 where nested window
   * functions within CASE expressions fail validation incorrectly. Only {@code
   * CalcitePPLEventstatsIT#testMultipleEventstatsWithNullBucket} should be caught by this check.
   *
   * <p><b>Calcite Bug (v1.41):</b> The {@code SqlImplementor.Result#containsOver()} method at
   * SqlImplementor.java:L2145 only checks {@code SqlBasicCall} nodes for window functions, missing
   * other {@code SqlCall} subclasses like {@code SqlCase}. This causes it to fail at detecting
   * window functions inside CASE expressions.
   *
   * <p><b>Impact:</b> When nested window functions exist (e.g., from double eventstats), Calcite's
   * {@code RelToSqlConverter} doesn't create the necessary subquery boundary because {@code
   * containsOver()} returns false for expressions like:
   *
   * <pre>
   * CASE WHEN ... THEN (SUM(age) OVER (...)) END
   * </pre>
   *
   * <p>This results in invalid SQL with nested aggregations:
   *
   * <pre>
   * SUM(CASE WHEN ... THEN (SUM(age) OVER (...)) END) OVER (...)
   * </pre>
   *
   * <p><b>TODO:</b> Remove this workaround when upgrading to a Calcite version that fixes the bug.
   *
   * @param e the exception to check
   * @return {@code true} if the exception should be tolerated as a known Calcite bug, {@code false}
   *     otherwise
   */
  public static boolean tolerantValidationException(Exception e) {
    return e.getMessage() != null
        && e.getMessage().contains("Aggregate expressions cannot be nested");
  }
}
