/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.analysis.symbol.SymbolTable;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

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

  public static final Map<String, ExprType> typeMapping =
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

  protected StorageEngine storageEngine() {
    return new StorageEngine() {
      @Override
      public Table getTable(DataSourceSchemaName dataSourceSchemaName, String name) {
        return new Table() {
          @Override
          public boolean exists() {
            return true;
          }

          @Override
          public void create(Map<String, ExprType> schema) {
            throw new UnsupportedOperationException("Create table is not supported");
          }

          @Override
          public Map<String, ExprType> getFieldTypes() {
            return typeMapping;
          }

          @Override
          public PhysicalPlan implement(LogicalPlan plan) {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  protected SymbolTable symbolTable() {
    SymbolTable symbolTable = new SymbolTable();
    typeMapping
        .entrySet()
        .forEach(
            entry ->
                symbolTable.store(
                    new Symbol(Namespace.FIELD_NAME, entry.getKey()), entry.getValue()));
    return symbolTable;
  }

  protected Environment<Expression, ExprType> typeEnv() {
    return var -> {
      if (var instanceof ReferenceExpression) {
        ReferenceExpression refExpr = (ReferenceExpression) var;
        if (typeMapping.containsKey(refExpr.getAttr())) {
          return typeMapping.get(refExpr.getAttr());
        }
      }
      throw new ExpressionEvaluationException("type resolved failed");
    };
  }
}
