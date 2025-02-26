/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.BOOLEAN;
import static org.opensearch.sql.data.type.ExprCoreType.BYTE;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.data.type.ExprCoreType.FLOAT;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.IP;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.SHORT;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.data.type.ExprCoreType.UNDEFINED;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.OpenSearchTypeSystem;
import org.opensearch.sql.storage.Table;

/** This class is used to create RelDataType and map RelDataType to Java data type */
public class OpenSearchTypeFactory extends JavaTypeFactoryImpl {
  public static final OpenSearchTypeFactory TYPE_FACTORY =
      new OpenSearchTypeFactory(OpenSearchTypeSystem.INSTANCE);

  private OpenSearchTypeFactory(RelDataTypeSystem typeSystem) {
    super(typeSystem);
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
        case STRING:
          return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable);
        case BOOLEAN:
          return TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN, nullable);
        case DATE:
          return TYPE_FACTORY.createSqlType(SqlTypeName.DATE, nullable);
        case TIME:
          return TYPE_FACTORY.createSqlType(SqlTypeName.TIME, nullable);
        case TIMESTAMP:
          return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, nullable);
        case ARRAY:
          return TYPE_FACTORY.createArrayType(
              TYPE_FACTORY.createSqlType(SqlTypeName.ANY, nullable), -1);
        case STRUCT:
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
        return TYPE_FACTORY.createSqlType(SqlTypeName.BINARY, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("timestamp")) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("geo_point")) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.GEOMETRY, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("text")) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable);
      } else if (fieldType.legacyTypeName().equalsIgnoreCase("ip")) {
        return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable); // TODO UDT
      } else {
        throw new IllegalArgumentException(
            "Unsupported conversion for OpenSearch Data type: " + fieldType.typeName());
      }
    }
  }

  /** Converts a Calcite data type to OpenSearch ExprCoreType. */
  public static ExprType convertRelDataTypeToExprType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
        return BYTE;
      case SMALLINT:
        return SHORT;
      case INTEGER:
        return INTEGER;
      case BIGINT:
        return LONG;
      case REAL:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case VARCHAR:
        return STRING;
      case BOOLEAN:
        return BOOLEAN;
      case DATE:
        return DATE;
      case TIME:
        return TIME;
      case TIMESTAMP:
        return TIMESTAMP;
      case GEOMETRY:
        return IP;
      case ARRAY:
        return ARRAY;
      case MAP:
        return STRUCT;
      case NULL:
        return UNDEFINED;
      default:
        throw new IllegalArgumentException(
            "Unsupported conversion for Relational Data type: " + type.getSqlTypeName());
    }
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
    for (Map.Entry<String, ExprType> entry : table.getFieldTypes().entrySet()) {
      fieldNameList.add(entry.getKey());
      typeList.add(OpenSearchTypeFactory.convertExprTypeToRelDataType(entry.getValue()));
    }
    return TYPE_FACTORY.createStructType(typeList, fieldNameList, true);
  }

  /** not in use for now, but let's keep this code for future reference. */
  @Override
  public Type getJavaClass(RelDataType type) {
    return super.getJavaClass(type);
  }
}
