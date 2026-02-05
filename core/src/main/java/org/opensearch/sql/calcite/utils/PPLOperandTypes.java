/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.Locale;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeUtil;
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
      UDFOperandMetadata.wrap(OperandTypes.INTEGER.or(OperandTypes.family()));
  public static final UDFOperandMetadata STRING = UDFOperandMetadata.wrap(OperandTypes.CHARACTER);
  public static final UDFOperandMetadata INTEGER = UDFOperandMetadata.wrap(OperandTypes.INTEGER);
  public static final UDFOperandMetadata NUMERIC = UDFOperandMetadata.wrap(OperandTypes.NUMERIC);

  public static final UDFOperandMetadata NUMERIC_OPTIONAL_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.NUMERIC.or(
              OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)));

  public static final UDFOperandMetadata ANY_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER)));
  public static final UDFOperandMetadata INTEGER_INTEGER =
      UDFOperandMetadata.wrap(OperandTypes.INTEGER_INTEGER);
  public static final UDFOperandMetadata STRING_STRING =
      UDFOperandMetadata.wrap(OperandTypes.CHARACTER_CHARACTER);
  public static final UDFOperandMetadata STRING_STRING_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));
  public static final UDFOperandMetadata NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap(OperandTypes.NUMERIC_NUMERIC);
  public static final UDFOperandMetadata STRING_INTEGER =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));
  public static final UDFOperandMetadata STRING_STRING_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));

  public static final UDFOperandMetadata STRING_OR_STRING_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.CHARACTER)
              .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER)));

  public static final UDFOperandMetadata STRING_STRING_INTEGER_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.CHARACTER,
              SqlTypeFamily.CHARACTER,
              SqlTypeFamily.INTEGER,
              SqlTypeFamily.INTEGER));

  public static final UDFOperandMetadata NUMERIC_STRING_OR_STRING_STRING =
      UDFOperandMetadata.wrap(
          (OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER))
              .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)));

  public static final UDFOperandMetadata NUMERIC_NUMERIC_OPTIONAL_NUMERIC_SYMBOL =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.NUMERIC,
                      SqlTypeFamily.ANY)));
  public static final UDFOperandMetadata NUMERIC_NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC));
  public static final UDFOperandMetadata NUMERIC_NUMERIC_NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC));

  public static final UDFOperandMetadata WIDTH_BUCKET_OPERAND =
      UDFOperandMetadata.wrap(
          // 1. Numeric fields: bin age span=10
          OperandTypes.family(
                  SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.INTEGER,
                  SqlTypeFamily.NUMERIC,
                  SqlTypeFamily.NUMERIC)
              // 2. Timestamp fields with OpenSearch type system
              // Used in: Production + Integration tests (CalciteBinCommandIT)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.TIMESTAMP,
                      SqlTypeFamily.INTEGER,
                      SqlTypeFamily.CHARACTER, // TIMESTAMP - TIMESTAMP = INTERVAL (as STRING)
                      SqlTypeFamily.TIMESTAMP))
              // 3. Timestamp fields with Calcite SCOTT schema
              // Used in: Unit tests (CalcitePPLBinTest)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.TIMESTAMP,
                      SqlTypeFamily.INTEGER,
                      SqlTypeFamily.TIMESTAMP, // TIMESTAMP - TIMESTAMP = TIMESTAMP
                      SqlTypeFamily.TIMESTAMP))
              // DATE field with OpenSearch type system
              // Used in: Production + Integration tests (CalciteBinCommandIT)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.DATE,
                      SqlTypeFamily.INTEGER,
                      SqlTypeFamily.CHARACTER, // DATE - DATE = INTERVAL (as STRING)
                      SqlTypeFamily.DATE))
              // DATE field with Calcite SCOTT schema
              // Used in: Unit tests (CalcitePPLBinTest)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.DATE,
                      SqlTypeFamily.INTEGER,
                      SqlTypeFamily.DATE, // DATE - DATE = DATE
                      SqlTypeFamily.DATE))
              // TIME field with OpenSearch type system
              // Used in: Production + Integration tests (CalciteBinCommandIT)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.TIME,
                      SqlTypeFamily.INTEGER,
                      SqlTypeFamily.CHARACTER, // TIME - TIME = INTERVAL (as STRING)
                      SqlTypeFamily.TIME))
              // TIME field with Calcite SCOTT schema
              // Used in: Unit tests (CalcitePPLBinTest)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.TIME,
                      SqlTypeFamily.INTEGER,
                      SqlTypeFamily.TIME, // TIME - TIME = TIME
                      SqlTypeFamily.TIME)));

  public static final UDFOperandMetadata NUMERIC_NUMERIC_NUMERIC_NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC,
              SqlTypeFamily.NUMERIC));
  public static final UDFOperandMetadata STRING_OR_INTEGER_INTEGER_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)));

  public static final UDFOperandMetadata OPTIONAL_DATE_OR_TIMESTAMP_OR_NUMERIC =
      UDFOperandMetadata.wrap(
          OperandTypes.DATETIME.or(OperandTypes.NUMERIC).or(OperandTypes.family()));

  public static final UDFOperandMetadata DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(OperandTypes.DATETIME.or(OperandTypes.CHARACTER));
  public static final UDFOperandMetadata TIME_OR_TIMESTAMP_OR_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.CHARACTER.or(OperandTypes.TIME).or(OperandTypes.TIMESTAMP));
  public static final UDFOperandMetadata DATE_OR_TIMESTAMP_OR_STRING =
      UDFOperandMetadata.wrap(OperandTypes.DATE_OR_TIMESTAMP.or(OperandTypes.CHARACTER));
  public static final UDFOperandMetadata DATETIME_OR_STRING_OR_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.DATETIME.or(OperandTypes.CHARACTER).or(OperandTypes.INTEGER));

  public static final UDFOperandMetadata DATETIME_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.DATETIME.or(
              OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)));

  public static final UDFOperandMetadata DATETIME_DATETIME =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME));
  public static final UDFOperandMetadata DATETIME_OR_STRING_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER)
              .or(OperandTypes.CHARACTER_CHARACTER));
  public static final UDFOperandMetadata STRING_TIMESTAMP =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP));
  public static final UDFOperandMetadata STRING_DATETIME =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME));
  public static final UDFOperandMetadata DATETIME_INTERVAL =
      UDFOperandMetadata.wrap(OperandTypes.DATETIME_INTERVAL);
  public static final UDFOperandMetadata TIME_TIME =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.TIME, SqlTypeFamily.TIME));

  public static final UDFOperandMetadata TIMESTAMP_OR_STRING_STRING_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
                  SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)));
  public static final UDFOperandMetadata STRING_INTEGER_DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
                  SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.DATETIME)));
  public static final UDFOperandMetadata INTERVAL_DATETIME_DATETIME =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
                  SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME)
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME))
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER))
              .or(
                  OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)));

  /**
   * Operand type checker that accepts any scalar type. This includes numeric types, strings,
   * booleans, datetime types, and special scalar types like IP and BINARY. Excludes complex types
   * like arrays, structs, and maps.
   */
  public static final UDFOperandMetadata SCALAR =
      UDFOperandMetadata.wrap(
          new SqlOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
              if (!getOperandCountRange().isValidCount(callBinding.getOperandCount())) {
                return false;
              }
              return OpenSearchTypeUtil.isScalar(callBinding.getOperandType(0));
            }

            @Override
            public SqlOperandCountRange getOperandCountRange() {
              return SqlOperandCountRanges.of(1);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
              return String.format(Locale.ROOT, "%s(<SCALAR>)", opName);
            }
          });

  /**
   * Operand type checker that accepts any scalar type with an optional integer argument. This is
   * used for aggregation functions that take a field and an optional limit/size parameter.
   */
  public static final UDFOperandMetadata SCALAR_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          new SqlOperandTypeChecker() {
            @Override
            public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
              if (!getOperandCountRange().isValidCount(callBinding.getOperandCount())) {
                return false;
              }
              boolean valid = OpenSearchTypeUtil.isScalar(callBinding.getOperandType(0));
              if (callBinding.getOperandCount() == 2) {
                valid = valid && SqlTypeUtil.isIntType(callBinding.getOperandType(1));
              }
              return valid;
            }

            @Override
            public SqlOperandCountRange getOperandCountRange() {
              return SqlOperandCountRanges.between(1, 2);
            }

            @Override
            public String getAllowedSignatures(SqlOperator op, String opName) {
              return String.format(
                  Locale.ROOT, "%s(<SCALAR>), %s(<SCALAR>, <INTEGER>)", opName, opName);
            }
          });
}
