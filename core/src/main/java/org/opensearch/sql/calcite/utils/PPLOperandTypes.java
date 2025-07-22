/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.calcite.utils;

import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * This class contains common operand types for PPL functions. They are created by either wrapping a
 * {@link FamilyOperandTypeChecker} or a {@link CompositeOperandTypeChecker} with a {@link
 * UDFOperandMetadata}.
 */
public class PPLOperandTypes {
  // This class is not meant to be instantiated.
  private PPLOperandTypes() {}

  public static final UDFOperandMetadata NONE = UDFOperandMetadata.wrap(OperandTypes.family());
  public static final UDFOperandMetadata OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.INTEGER.or(OperandTypes.family()));
  public static final UDFOperandMetadata STRING =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.CHARACTER);
  public static final UDFOperandMetadata INTEGER =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.INTEGER);
  public static final UDFOperandMetadata NUMERIC =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.NUMERIC);
  public static final UDFOperandMetadata INTEGER_INTEGER =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.INTEGER_INTEGER);
  public static final UDFOperandMetadata STRING_STRING =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.CHARACTER_CHARACTER);
  public static final UDFOperandMetadata NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.NUMERIC_NUMERIC);
  public static final UDFOperandMetadata NUMERIC_NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC));

  public static final UDFOperandMetadata DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.DATETIME.or(OperandTypes.CHARACTER));
  public static final UDFOperandMetadata DATETIME_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.DATETIME.or(
                  OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)));
  public static final UDFOperandMetadata DATETIME_DATETIME =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME));
  public static final UDFOperandMetadata DATETIME_OR_STRING_DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.CHARACTER_CHARACTER
                  .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME))
                  .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER))
                  .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME)));
  public static final UDFOperandMetadata TIME_OR_TIMESTAMP_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.CHARACTER.or(OperandTypes.TIME).or(OperandTypes.TIMESTAMP));
  public static final UDFOperandMetadata DATE_OR_TIMESTAMP_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.DATE_OR_TIMESTAMP.or(OperandTypes.CHARACTER));
  public static final UDFOperandMetadata STRING_TIMESTAMP =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP));

  public static final UDFOperandMetadata TIMESTAMP_STRING_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));

  public static final UDFOperandMetadata STRING_INTEGER =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));
}
