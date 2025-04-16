/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BINARY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.GEO_POINT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.IP;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;
import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;
import static org.opensearch.sql.executor.QueryType.PPL;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;

import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Getter;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.type.ExprBinaryType;
import org.opensearch.sql.calcite.type.ExprDateType;
import org.opensearch.sql.calcite.type.ExprIPType;
import org.opensearch.sql.calcite.type.ExprTimeStampType;
import org.opensearch.sql.calcite.type.ExprTimeType;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.OpenSearchTypeSystem;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.storage.Table;

/** This class is used to create RelDataType and map RelDataType to Java data type */
public class OpenSearchTypeFactory extends JavaTypeFactoryImpl {
  public static final OpenSearchTypeFactory TYPE_FACTORY =
      new OpenSearchTypeFactory(OpenSearchTypeSystem.INSTANCE);

  private OpenSearchTypeFactory(RelDataTypeSystem typeSystem) {
    super(typeSystem);
  }

  @Getter
  public enum ExprUDT {
    EXPR_DATE(DATE),
    EXPR_TIME(TIME),
    EXPR_TIMESTAMP(TIMESTAMP),
    EXPR_BINARY(BINARY),
    EXPR_IP(IP);

    // Associated `ExprCoreType`
    private final ExprCoreType exprCoreType;

    ExprUDT(ExprCoreType exprCoreType) {
      this.exprCoreType = exprCoreType;
    }
  }

  @Override
  public RelDataType createTypeWithNullability(RelDataType type, boolean nullable) {
    if (type instanceof AbstractExprRelDataType<?> udt) {
      return udt.createWithNullability(this, nullable);
    }
    return super.createTypeWithNullability(type, nullable);
  }

  @Override
  public RelDataType createTypeWithCharsetAndCollation(
      RelDataType type, Charset charset, SqlCollation collation) {
    if (type instanceof AbstractExprRelDataType<?> udt) {
      return udt.createWithCharsetAndCollation(this, charset, collation);
    }
    return super.createTypeWithCharsetAndCollation(type, charset, collation);
  }

  public RelDataType createSqlType(SqlTypeName typeName, boolean nullable) {
    return createTypeWithNullability(super.createSqlType(typeName), nullable);
  }

  public RelDataType createStructType(
      List<RelDataType> typeList, List<String> fieldNameList, boolean nullable) {
    return createTypeWithNullability(super.createStructType(typeList, fieldNameList), nullable);
  }

  public RelDataType createMultisetType(RelDataType type, long maxCardinality, boolean nullable) {
    return createTypeWithNullability(super.createMultisetType(type, maxCardinality), nullable);
  }

  public RelDataType createMapType(RelDataType keyType, RelDataType valueType, boolean nullable) {
    return createTypeWithNullability(super.createMapType(keyType, valueType), nullable);
  }

  public RelDataType createUDT(ExprUDT typeName) {
    RelDataType udt =
        switch (typeName) {
          case EXPR_DATE:
            yield new ExprDateType(this);
          case EXPR_TIME:
            yield new ExprTimeType(this);
          case EXPR_TIMESTAMP:
            yield new ExprTimeStampType(this);
          case EXPR_BINARY:
            yield new ExprBinaryType(this);
          case EXPR_IP:
            yield new ExprIPType(this);
        };
    return canonize(SqlTypeUtil.addCharsetAndCollation(udt, this));
  }

  public RelDataType createUDT(ExprUDT typeName, boolean nullable) {
    return this.createTypeWithNullability(createUDT(typeName), nullable);
  }

  public static RelDataType convertExprTypeToRelDataType(ExprType field) {
    return convertExprTypeToRelDataType(field, true);
  }

  /** Converts a OpenSearch ExprCoreType field to relational type. */
  public static RelDataType convertExprTypeToRelDataType(ExprType fieldType, boolean nullable) {
    if (fieldType instanceof ExprCoreType) {
      switch ((ExprCoreType) fieldType) {
        case UNDEFINED:
          return TYPE_FACTORY.createSqlType(SqlTypeName.NULL, nullable);
        case BYTE:
          return TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT, nullable);
        case SHORT:
          return TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT, nullable);
        case INTEGER:
          return TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER, nullable);
        case LONG:
          return TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT, nullable);
        case FLOAT:
          return TYPE_FACTORY.createSqlType(SqlTypeName.REAL, nullable);
        case DOUBLE:
          return TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE, nullable);
        case IP:
          return TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP, nullable);
        case STRING:
          return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable);
        case BOOLEAN:
          return TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN, nullable);
        case DATE:
          return TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE, nullable);
        case TIME:
          return TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME, nullable);
        case TIMESTAMP:
          return TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, nullable);
        case ARRAY:
          return TYPE_FACTORY.createArrayType(
              TYPE_FACTORY.createSqlType(SqlTypeName.ANY, nullable), -1);
        case STRUCT:
          // TODO: should use RelRecordType instead of MapSqlType here
          // https://github.com/opensearch-project/sql/issues/3459
          final RelDataType relKey = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
          return TYPE_FACTORY.createMapType(
              relKey, TYPE_FACTORY.createSqlType(SqlTypeName.BINARY), nullable);
        case UNKNOWN:
        default:
          throw new IllegalArgumentException(
              "Unsupported conversion for OpenSearch Data type: " + fieldType.typeName());
      }
    } else {
      if (fieldType.legacyTypeName().equalsIgnoreCase("binary")) {
        return TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("timestamp")) {
        return TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIMESTAMP, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("date")) {
        return TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("time")) {
        return TYPE_FACTORY.createUDT(ExprUDT.EXPR_TIME, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("geo_point")) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.GEOMETRY, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("text")) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("ip")) {
        return TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP, nullable);
      } else if (fieldType.getOriginalPath().isPresent()) {
        return convertExprTypeToRelDataType(fieldType.getOriginalExprType(), nullable);
      } else {
        throw new IllegalArgumentException(
            "Unsupported conversion for OpenSearch Data type: " + fieldType.typeName());
      }
    }
  }

  /**
   * Usually, {@link this#createSqlType(SqlTypeName, boolean)} is used to create RelDataType, then
   * convert it to ExprType. This is a util to convert when you don't have typeFactory. So they are
   * all ExprCoreType.
   */
  public static ExprType convertSqlTypeNameToExprType(SqlTypeName sqlTypeName) {
    return switch (sqlTypeName) {
      case TINYINT -> BYTE;
      case SMALLINT -> SHORT;
      case INTEGER -> INTEGER;
      case BIGINT -> LONG;
      case FLOAT, REAL -> FLOAT;
      case DOUBLE -> DOUBLE;
      case CHAR, VARCHAR -> STRING;
      case BOOLEAN -> BOOLEAN;
      case DATE -> DATE;
      case TIME, TIME_TZ, TIME_WITH_LOCAL_TIME_ZONE -> TIME;
      case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_TZ -> TIMESTAMP;
      case INTERVAL_YEAR,
          INTERVAL_YEAR_MONTH,
          INTERVAL_MONTH,
          INTERVAL_DAY,
          INTERVAL_DAY_HOUR,
          INTERVAL_DAY_MINUTE,
          INTERVAL_DAY_SECOND,
          INTERVAL_HOUR,
          INTERVAL_HOUR_MINUTE,
          INTERVAL_HOUR_SECOND,
          INTERVAL_MINUTE,
          INTERVAL_MINUTE_SECOND,
          INTERVAL_SECOND -> INTERVAL;
      case ARRAY -> ARRAY;
      case MAP -> STRUCT;
      case GEOMETRY -> GEO_POINT;
      case NULL -> UNDEFINED;
      default -> UNKNOWN;
    };
  }

  /** Get legacy name for a RelDataType. */
  public static String getLegacyTypeName(RelDataType relDataType, QueryType queryType) {
    ExprType type = convertRelDataTypeToExprType(relDataType);
    return (queryType == PPL ? PPL_SPEC.typeName(type) : type.legacyTypeName())
        .toUpperCase(Locale.ROOT);
  }

  /** Converts a Calcite data type to OpenSearch ExprCoreType. */
  public static ExprType convertRelDataTypeToExprType(RelDataType type) {
    if (type instanceof AbstractExprRelDataType<?> udt) {
      return udt.getExprType();
    }
    ExprType exprType = convertSqlTypeNameToExprType(type.getSqlTypeName());
    if (exprType == UNKNOWN) {
      throw new IllegalArgumentException(
          "Unsupported conversion for Relational Data type: " + type.getSqlTypeName());
    }
    return exprType;
  }

  public static ExprValue getExprValueByExprType(ExprType type, Object value) {
    switch (type) {
      case UNDEFINED:
        return ExprValueUtils.nullValue();
      case BYTE:
        return ExprValueUtils.byteValue((Byte) value);
      case SHORT:
        return ExprValueUtils.shortValue((Short) value);
      case INTEGER:
        return ExprValueUtils.integerValue((Integer) value);
      case LONG:
        return ExprValueUtils.longValue((Long) value);
      case FLOAT:
        return ExprValueUtils.floatValue((Float) value);
      case DOUBLE:
        return ExprValueUtils.doubleValue((Double) value);
      case STRING:
        return ExprValueUtils.stringValue((String) value);
      case BOOLEAN:
        return ExprValueUtils.booleanValue((Boolean) value);
      case DATE:
      case TIME:
      case TIMESTAMP:
        return ExprValueUtils.fromObjectValue(value);
      case IP:
        return ExprValueUtils.ipValue((String) value);
      case ARRAY:
        return ExprValueUtils.collectionValue((List<Object>) value);
      case STRUCT:
        return ExprValueUtils.tupleValue((Map<String, Object>) value);
      default:
        throw new IllegalArgumentException(
            "Unsupported conversion for OpenSearch Data type: " + type.typeName());
    }
  }

  public static RelDataType convertSchema(Table table) {
    List<String> fieldNameList = new ArrayList<>();
    List<RelDataType> typeList = new ArrayList<>();
    for (Entry<String, ExprType> entry : table.getFieldTypes().entrySet()) {
      fieldNameList.add(entry.getKey());
      typeList.add(OpenSearchTypeFactory.convertExprTypeToRelDataType(entry.getValue()));
    }
    return TYPE_FACTORY.createStructType(typeList, fieldNameList, true);
  }

  /** not in use for now, but let's keep this code for future reference. */
  @Override
  public Type getJavaClass(RelDataType type) {
    if (type instanceof AbstractExprRelDataType<?> exprRelDataType) {
      return exprRelDataType.getJavaType();
    }
    return super.getJavaClass(type);
  }
}
