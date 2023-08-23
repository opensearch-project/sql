/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/** Configuration will be used for UT. */
public class TestConfig {
  public static final String INT_TYPE_NULL_VALUE_FIELD = "int_null_value";
  public static final String INT_TYPE_MISSING_VALUE_FIELD = "int_missing_value";
  public static final String DOUBLE_TYPE_NULL_VALUE_FIELD = "double_null_value";
  public static final String DOUBLE_TYPE_MISSING_VALUE_FIELD = "double_missing_value";
  public static final String BOOL_TYPE_NULL_VALUE_FIELD = "null_value_boolean";
  public static final String BOOL_TYPE_MISSING_VALUE_FIELD = "missing_value_boolean";
  public static final String STRING_TYPE_NULL_VALUE_FIELD = "string_null_value";
  public static final String STRING_TYPE_MISSING_VALUE_FIELD = "string_missing_value";

  public static Map<String, ExprType> typeMapping =
      new ImmutableMap.Builder<String, ExprType>()
          .put("integer_value", ExprCoreType.INTEGER)
          .put(INT_TYPE_NULL_VALUE_FIELD, ExprCoreType.INTEGER)
          .put(INT_TYPE_MISSING_VALUE_FIELD, ExprCoreType.INTEGER)
          .put("long_value", ExprCoreType.LONG)
          .put("float_value", ExprCoreType.FLOAT)
          .put("double_value", ExprCoreType.DOUBLE)
          .put(DOUBLE_TYPE_NULL_VALUE_FIELD, ExprCoreType.DOUBLE)
          .put(DOUBLE_TYPE_MISSING_VALUE_FIELD, ExprCoreType.DOUBLE)
          .put("boolean_value", ExprCoreType.BOOLEAN)
          .put(BOOL_TYPE_NULL_VALUE_FIELD, ExprCoreType.BOOLEAN)
          .put(BOOL_TYPE_MISSING_VALUE_FIELD, ExprCoreType.BOOLEAN)
          .put("string_value", ExprCoreType.STRING)
          .put(STRING_TYPE_NULL_VALUE_FIELD, ExprCoreType.STRING)
          .put(STRING_TYPE_MISSING_VALUE_FIELD, ExprCoreType.STRING)
          .put("struct_value", ExprCoreType.STRUCT)
          .put("array_value", ExprCoreType.ARRAY)
          .put("timestamp_value", ExprCoreType.TIMESTAMP)
          .put("field_value1", ExprCoreType.STRING)
          .put("field_value2", ExprCoreType.STRING)
          .put("message", ExprCoreType.STRING)
          .put("message.info", ExprCoreType.STRING)
          .put("message.info.id", ExprCoreType.STRING)
          .put("comment", ExprCoreType.STRING)
          .put("comment.data", ExprCoreType.STRING)
          .build();
}
