/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.CompositeOperandTypeChecker;
import org.apache.calcite.sql.type.FamilyOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * This class contains common operand types for PPL functions. They are created by either wrapping a
 * {@link FamilyOperandTypeChecker} or a {@link CompositeOperandTypeChecker} with a {@link
 * UDFOperandMetadata}.
 */
public class PPLOperandTypes {
  // This class is not meant to be instantiated.
  private PPLOperandTypes() {}

  // Convenience RelDataType constants used to express UDF signatures via wrapUDT(...).
  // UDT-backed scalar types:
  public static final RelDataType DATE_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE);
  public static final RelDataType TIME_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME);
  public static final RelDataType TIMESTAMP_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP);
  public static final RelDataType IP_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
  public static final RelDataType BINARY_UDT = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
  // Plain SQL scalar types:
  public static final RelDataType BYTE_T = TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT);
  public static final RelDataType SHORT_T = TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT);
  public static final RelDataType INTEGER_T = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
  public static final RelDataType LONG_T = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
  public static final RelDataType FLOAT_T = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);
  public static final RelDataType DOUBLE_T = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);
  public static final RelDataType STRING_T = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
  public static final RelDataType BOOLEAN_T = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);

  /** List of all scalar type signatures (single parameter each) */
  private static final List<List<RelDataType>> SCALAR_TYPES =
      List.of(
          // Numeric types
          List.of(BYTE_T),
          List.of(SHORT_T),
          List.of(INTEGER_T),
          List.of(LONG_T),
          List.of(FLOAT_T),
          List.of(DOUBLE_T),
          // String type
          List.of(STRING_T),
          // Boolean type
          List.of(BOOLEAN_T),
          // Temporal types
          List.of(DATE_UDT),
          List.of(TIME_UDT),
          List.of(TIMESTAMP_UDT),
          // Special scalar types
          List.of(IP_UDT),
          List.of(BINARY_UDT));

  /** Helper method to create scalar types with optional integer parameter */
  private static List<List<RelDataType>> createScalarWithOptionalInteger() {
    List<List<RelDataType>> result = new ArrayList<>(SCALAR_TYPES);

    // Add scalar + integer combinations
    SCALAR_TYPES.forEach(scalarType -> result.add(List.of(scalarType.get(0), INTEGER_T)));

    return result;
  }

  public static final UDFOperandMetadata NONE = UDFOperandMetadata.wrap(OperandTypes.family());
  public static final UDFOperandMetadata OPTIONAL_ANY =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(SqlTypeFamily.ANY).or(OperandTypes.family()));
  public static final UDFOperandMetadata OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.INTEGER.or(OperandTypes.family()));
  public static final UDFOperandMetadata STRING =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.CHARACTER);
  public static final UDFOperandMetadata INTEGER =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.INTEGER);
  public static final UDFOperandMetadata NUMERIC =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.NUMERIC);

  public static final UDFOperandMetadata NUMERIC_OPTIONAL_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.NUMERIC.or(
                  OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)));

  public static final UDFOperandMetadata ANY_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER)));
  public static final UDFOperandMetadata ANY_OPTIONAL_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER)));
  public static final UDFOperandMetadata ANY_OPTIONAL_TIMESTAMP =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.ANY.or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.TIMESTAMP)));
  public static final UDFOperandMetadata INTEGER_INTEGER =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.INTEGER_INTEGER);
  public static final UDFOperandMetadata STRING_STRING =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.CHARACTER_CHARACTER);
  public static final UDFOperandMetadata STRING_STRING_STRING =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));
  public static final UDFOperandMetadata NUMERIC_NUMERIC =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.NUMERIC_NUMERIC);
  public static final UDFOperandMetadata STRING_INTEGER =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));
  public static final UDFOperandMetadata STRING_STRING_INTEGER =
      UDFOperandMetadata.wrap(
          OperandTypes.family(
              SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER));

  public static final UDFOperandMetadata STRING_OR_STRING_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
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
          (CompositeOperandTypeChecker)
              (OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING))
                  .or(OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING)));

  public static final UDFOperandMetadata NUMERIC_NUMERIC_OPTIONAL_NUMERIC =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.NUMERIC_NUMERIC.or(
                  OperandTypes.family(
                      SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)));
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
          (CompositeOperandTypeChecker)
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
          (CompositeOperandTypeChecker)
              OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER)));

  public static final UDFOperandMetadata OPTIONAL_DATE_OR_TIMESTAMP_OR_NUMERIC =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.DATETIME.or(OperandTypes.NUMERIC).or(OperandTypes.family()));

  public static final UDFOperandMetadata DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.DATETIME.or(OperandTypes.CHARACTER));
  public static final UDFOperandMetadata TIME_OR_TIMESTAMP_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.CHARACTER.or(OperandTypes.TIME).or(OperandTypes.TIMESTAMP));
  public static final UDFOperandMetadata DATE_OR_TIMESTAMP_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker) OperandTypes.DATE_OR_TIMESTAMP.or(OperandTypes.CHARACTER));
  public static final UDFOperandMetadata DATETIME_OR_STRING_OR_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.DATETIME.or(OperandTypes.CHARACTER).or(OperandTypes.INTEGER));

  public static final UDFOperandMetadata DATETIME_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.DATETIME.or(
                  OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.INTEGER)));
  public static final UDFOperandMetadata ANY_DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(SqlTypeFamily.ANY)
                  .or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.DATETIME))
                  .or(OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.STRING)));

  public static final UDFOperandMetadata DATETIME_DATETIME =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME));
  public static final UDFOperandMetadata DATETIME_OR_STRING_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER)
                  .or(OperandTypes.CHARACTER_CHARACTER));
  public static final UDFOperandMetadata DATETIME_OR_STRING_DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.CHARACTER_CHARACTER
                  .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME))
                  .or(OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.CHARACTER))
                  .or(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME)));
  public static final UDFOperandMetadata STRING_TIMESTAMP =
      UDFOperandMetadata.wrap(
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.TIMESTAMP));
  public static final UDFOperandMetadata STRING_DATETIME =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME));
  public static final UDFOperandMetadata DATETIME_INTERVAL =
      UDFOperandMetadata.wrap((FamilyOperandTypeChecker) OperandTypes.DATETIME_INTERVAL);
  public static final UDFOperandMetadata TIME_TIME =
      UDFOperandMetadata.wrap(OperandTypes.family(SqlTypeFamily.TIME, SqlTypeFamily.TIME));

  public static final UDFOperandMetadata TIMESTAMP_OR_STRING_STRING_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(
                      SqlTypeFamily.TIMESTAMP, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.CHARACTER,
                          SqlTypeFamily.CHARACTER,
                          SqlTypeFamily.CHARACTER)));
  public static final UDFOperandMetadata STRING_INTEGER_DATETIME_OR_STRING =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
              OperandTypes.family(
                      SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.CHARACTER)
                  .or(
                      OperandTypes.family(
                          SqlTypeFamily.CHARACTER, SqlTypeFamily.INTEGER, SqlTypeFamily.DATETIME)));
  public static final UDFOperandMetadata INTERVAL_DATETIME_DATETIME =
      UDFOperandMetadata.wrap(
          (CompositeOperandTypeChecker)
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
                          SqlTypeFamily.CHARACTER,
                          SqlTypeFamily.CHARACTER,
                          SqlTypeFamily.CHARACTER)));

  /**
   * Operand type checker that accepts any scalar type. This includes numeric types, strings,
   * booleans, datetime types, and special scalar types like IP and BINARY. Excludes complex types
   * like arrays, structs, and maps.
   */
  public static final UDFOperandMetadata ANY_SCALAR = UDFOperandMetadata.wrapUDT(SCALAR_TYPES);

  /**
   * Operand type checker that accepts any scalar type with an optional integer argument. This is
   * used for aggregation functions that take a field and an optional limit/size parameter.
   */
  public static final UDFOperandMetadata ANY_SCALAR_OPTIONAL_INTEGER =
      UDFOperandMetadata.wrapUDT(createScalarWithOptionalInteger());
}
