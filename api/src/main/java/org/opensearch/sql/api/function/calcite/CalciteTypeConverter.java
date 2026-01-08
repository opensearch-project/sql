/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.function.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Utility for converting between Calcite types and SQL type names.
 *
 * <p>This converter provides bidirectional conversion between Calcite's {@link RelDataType} and
 * engine-agnostic SQL type name strings. This enables type information to be serialized and shared
 * across different execution engines without Calcite-specific dependencies.
 *
 * <h2>Supported Type Conversions</h2>
 *
 * <ul>
 *   <li><b>Primitive types:</b> VARCHAR, INTEGER, BIGINT, DOUBLE, BOOLEAN, DATE, TIMESTAMP
 *   <li><b>Array types:</b> ARRAY&lt;ELEMENT_TYPE&gt; (e.g., "ARRAY&lt;INTEGER&gt;")
 *   <li><b>Struct types:</b> STRUCT&lt;field1:TYPE1, field2:TYPE2&gt; (e.g.,
 *       "STRUCT&lt;name:VARCHAR, age:INTEGER&gt;")
 *   <li><b>Unknown types:</b> "UNKNOWN" for types that cannot be mapped
 * </ul>
 *
 * <h2>Example Usage</h2>
 *
 * <pre>{@code
 * // Convert Calcite type to SQL type name
 * RelDataType calciteType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
 * String typeName = CalciteTypeConverter.relDataTypeToSqlTypeName(calciteType);
 * // typeName = "VARCHAR"
 *
 * // Convert SQL type name back to Calcite type
 * RelDataType reconstructed = CalciteTypeConverter.toCalciteType("VARCHAR", typeFactory);
 * // reconstructed is equivalent to calciteType
 *
 * // Complex type example
 * RelDataType arrayType = typeFactory.createArrayType(
 *     typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
 * String arrayTypeName = CalciteTypeConverter.relDataTypeToSqlTypeName(arrayType);
 * // arrayTypeName = "ARRAY<INTEGER>"
 * }</pre>
 */
public class CalciteTypeConverter {

  // Pattern for parsing ARRAY types: ARRAY<ELEMENT_TYPE>
  private static final Pattern ARRAY_PATTERN = Pattern.compile("^ARRAY<(.+)>$");

  // Pattern for parsing STRUCT types: STRUCT<field1:TYPE1, field2:TYPE2, ...>
  private static final Pattern STRUCT_PATTERN = Pattern.compile("^STRUCT<(.+)>$");

  /**
   * Converts a Calcite {@link RelDataType} to a SQL type name string.
   *
   * <p>This method handles primitive types, array types, and struct types. For unknown or
   * unsupported types, it returns "UNKNOWN".
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>VARCHAR type → "VARCHAR"
   *   <li>INTEGER type → "INTEGER"
   *   <li>ARRAY(INTEGER) → "ARRAY&lt;INTEGER&gt;"
   *   <li>ROW(name VARCHAR, age INTEGER) → "STRUCT&lt;name:VARCHAR, age:INTEGER&gt;"
   * </ul>
   *
   * @param relDataType the Calcite type to convert, must not be null
   * @return SQL type name string, never null (returns "UNKNOWN" for unmapped types)
   * @throws NullPointerException if relDataType is null
   */
  public static String relDataTypeToSqlTypeName(RelDataType relDataType) {
    if (relDataType == null) {
      throw new NullPointerException("relDataType must not be null");
    }

    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();

    // Handle array types
    if (sqlTypeName == SqlTypeName.ARRAY) {
      RelDataType componentType = relDataType.getComponentType();
      if (componentType != null) {
        String elementTypeName = relDataTypeToSqlTypeName(componentType);
        return "ARRAY<" + elementTypeName + ">";
      }
      return "ARRAY<UNKNOWN>";
    }

    // Handle struct/row types
    if (sqlTypeName == SqlTypeName.ROW) {
      List<RelDataTypeField> fields = relDataType.getFieldList();
      if (fields.isEmpty()) {
        return "STRUCT<>";
      }

      StringBuilder sb = new StringBuilder("STRUCT<");
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        RelDataTypeField field = fields.get(i);
        sb.append(field.getName()).append(":").append(relDataTypeToSqlTypeName(field.getType()));
      }
      sb.append(">");
      return sb.toString();
    }

    // Handle primitive types
    switch (sqlTypeName) {
      case VARCHAR:
      case CHAR:
        return "VARCHAR";
      case INTEGER:
        return "INTEGER";
      case BIGINT:
        return "BIGINT";
      case DOUBLE:
      case FLOAT:
      case REAL:
        return "DOUBLE";
      case BOOLEAN:
        return "BOOLEAN";
      case DATE:
        return "DATE";
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "TIMESTAMP";
      default:
        return "UNKNOWN";
    }
  }

  /**
   * Converts a SQL type name string to a Calcite {@link RelDataType}.
   *
   * <p>This method parses SQL type name strings and creates corresponding Calcite types using the
   * provided type factory. It supports primitive types, array types, and struct types.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>"VARCHAR" → VARCHAR type
   *   <li>"INTEGER" → INTEGER type
   *   <li>"ARRAY&lt;INTEGER&gt;" → ARRAY(INTEGER) type
   *   <li>"STRUCT&lt;name:VARCHAR, age:INTEGER&gt;" → ROW(name VARCHAR, age INTEGER) type
   * </ul>
   *
   * @param sqlTypeName the SQL type name string to parse, must not be null
   * @param typeFactory the Calcite type factory for creating types, must not be null
   * @return the corresponding Calcite type, never null
   * @throws IllegalArgumentException if the type name is invalid or unsupported
   * @throws NullPointerException if sqlTypeName or typeFactory is null
   */
  public static RelDataType toCalciteType(String sqlTypeName, RelDataTypeFactory typeFactory) {
    if (sqlTypeName == null) {
      throw new NullPointerException("sqlTypeName must not be null");
    }
    if (typeFactory == null) {
      throw new NullPointerException("typeFactory must not be null");
    }

    String trimmedTypeName = sqlTypeName.trim();

    // Handle array types
    Matcher arrayMatcher = ARRAY_PATTERN.matcher(trimmedTypeName);
    if (arrayMatcher.matches()) {
      String elementTypeName = arrayMatcher.group(1);
      RelDataType elementType = toCalciteType(elementTypeName, typeFactory);
      return typeFactory.createArrayType(elementType, -1);
    }

    // Handle struct types
    Matcher structMatcher = STRUCT_PATTERN.matcher(trimmedTypeName);
    if (structMatcher.matches()) {
      String fieldsStr = structMatcher.group(1);
      if (fieldsStr.isEmpty()) {
        // Empty struct
        return typeFactory.createStructType(new ArrayList<>(), new ArrayList<>());
      }

      List<String> fieldNames = new ArrayList<>();
      List<RelDataType> fieldTypes = new ArrayList<>();

      // Parse field definitions
      List<String> fieldDefs = parseStructFields(fieldsStr);
      for (String fieldDef : fieldDefs) {
        int colonIndex = fieldDef.indexOf(':');
        if (colonIndex == -1) {
          throw new IllegalArgumentException(
              "Invalid struct field definition: " + fieldDef + " (expected format: name:TYPE)");
        }

        String fieldName = fieldDef.substring(0, colonIndex).trim();
        String fieldTypeName = fieldDef.substring(colonIndex + 1).trim();

        if (fieldName.isEmpty()) {
          throw new IllegalArgumentException("Struct field name cannot be empty");
        }

        fieldNames.add(fieldName);
        fieldTypes.add(toCalciteType(fieldTypeName, typeFactory));
      }

      return typeFactory.createStructType(fieldTypes, fieldNames);
    }

    // Handle primitive types
    switch (trimmedTypeName.toUpperCase()) {
      case "VARCHAR":
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case "INTEGER":
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case "BIGINT":
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case "DOUBLE":
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case "BOOLEAN":
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case "DATE":
        return typeFactory.createSqlType(SqlTypeName.DATE);
      case "TIMESTAMP":
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
      case "UNKNOWN":
        return typeFactory.createSqlType(SqlTypeName.ANY);
      default:
        throw new IllegalArgumentException("Unsupported SQL type name: " + trimmedTypeName);
    }
  }

  /**
   * Parses struct field definitions from a comma-separated string.
   *
   * <p>This method handles nested types correctly by tracking angle bracket depth to avoid
   * splitting on commas inside nested type definitions.
   *
   * <p>Example: "name:VARCHAR, address:STRUCT&lt;street:VARCHAR, city:VARCHAR&gt;, age:INTEGER"
   * will be split into:
   *
   * <ul>
   *   <li>"name:VARCHAR"
   *   <li>"address:STRUCT&lt;street:VARCHAR, city:VARCHAR&gt;"
   *   <li>"age:INTEGER"
   * </ul>
   *
   * @param fieldsStr the comma-separated field definitions
   * @return list of individual field definition strings
   */
  private static List<String> parseStructFields(String fieldsStr) {
    List<String> fields = new ArrayList<>();
    StringBuilder currentField = new StringBuilder();
    int depth = 0;

    for (char c : fieldsStr.toCharArray()) {
      if (c == '<') {
        depth++;
        currentField.append(c);
      } else if (c == '>') {
        depth--;
        currentField.append(c);
      } else if (c == ',' && depth == 0) {
        // Top-level comma - this is a field separator
        fields.add(currentField.toString().trim());
        currentField = new StringBuilder();
      } else {
        currentField.append(c);
      }
    }

    // Add the last field
    if (currentField.length() > 0) {
      fields.add(currentField.toString().trim());
    }

    return fields;
  }
}
