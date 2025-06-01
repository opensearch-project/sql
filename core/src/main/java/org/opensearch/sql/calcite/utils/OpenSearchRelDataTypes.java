/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.storage.Table;

public class OpenSearchRelDataTypes extends JavaTypeFactoryImpl {
  public static final OpenSearchRelDataTypes TYPE_FACTORY =
      new OpenSearchRelDataTypes(RelDataTypeSystem.DEFAULT);

  private OpenSearchRelDataTypes(RelDataTypeSystem typeSystem) {
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

  public static RelDataType convertSchemaField(ExprType field) {
    return convertSchemaField(field, true);
  }

  /** Converts a OpenSearch ExprCoreType field to relational type. */
  public static RelDataType convertSchemaField(ExprType fieldType, boolean nullable) {
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
        return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, nullable);
      } else {
        throw new IllegalArgumentException(
            "Unsupported conversion for OpenSearch Data type: " + fieldType.typeName());
      }
    }
  }

  public static RelDataType convertSchema(Table table) {
    List<String> fieldNameList = new ArrayList<>();
    List<RelDataType> typeList = new ArrayList<>();
    for (Map.Entry<String, ExprType> entry : table.getFieldTypes().entrySet()) {
      fieldNameList.add(entry.getKey());
      typeList.add(OpenSearchRelDataTypes.convertSchemaField(entry.getValue()));
    }
    return TYPE_FACTORY.createStructType(typeList, fieldNameList, true);
  }
}
