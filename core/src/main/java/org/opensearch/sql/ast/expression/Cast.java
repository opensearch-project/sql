/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_BOOLEAN;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_BYTE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_DATE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_DATETIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_DOUBLE;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_FLOAT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_INT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_IP;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_LONG;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_SHORT;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_STRING;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_TIME;
import static org.opensearch.sql.expression.function.BuiltinFunctionName.CAST_TO_TIMESTAMP;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.FunctionName;

/** AST node that represents Cast clause. */
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class Cast extends UnresolvedExpression {

  private static final Map<String, FunctionName> CONVERTED_TYPE_FUNCTION_NAME_MAP =
      new ImmutableMap.Builder<String, FunctionName>()
          .put("string", CAST_TO_STRING.getName())
          .put("byte", CAST_TO_BYTE.getName())
          .put("short", CAST_TO_SHORT.getName())
          .put("int", CAST_TO_INT.getName())
          .put("integer", CAST_TO_INT.getName())
          .put("long", CAST_TO_LONG.getName())
          .put("float", CAST_TO_FLOAT.getName())
          .put("double", CAST_TO_DOUBLE.getName())
          .put("boolean", CAST_TO_BOOLEAN.getName())
          .put("date", CAST_TO_DATE.getName())
          .put("time", CAST_TO_TIME.getName())
          .put("timestamp", CAST_TO_TIMESTAMP.getName())
          .put("datetime", CAST_TO_DATETIME.getName())
          .put("ip", CAST_TO_IP.getName())
          .build();

  /** The source expression cast from. */
  private final UnresolvedExpression expression;

  /** Expression that represents name of the target type. */
  private final UnresolvedExpression convertedType;

  /**
   * Check if the given function name is a cast function or not.
   *
   * @param name function name
   * @return true if cast function, otherwise false.
   */
  public static boolean isCastFunction(FunctionName name) {
    return CONVERTED_TYPE_FUNCTION_NAME_MAP.containsValue(name);
  }

  /** Get the data type expression of the converted type. */
  public DataType getDataType() {
    String type = convertedType.toString().toUpperCase(Locale.ROOT);
    if ("INT".equals(type)) {
      type = "INTEGER";
    }
    // JSON is not a data type for now, we convert it to STRING.
    // TODO Maybe its data type could be VARIANT in future?
    if ("JSON".equals(type)) {
      type = "STRING";
    }
    return DataType.valueOf(type);
  }

  /**
   * Get the cast function name for a given target data type.
   *
   * @param targetType target data type
   * @return cast function name corresponding
   */
  public static FunctionName getCastFunctionName(ExprType targetType) {
    String type = targetType.typeName().toLowerCase(Locale.ROOT);
    return CONVERTED_TYPE_FUNCTION_NAME_MAP.get(type);
  }

  /**
   * Get the converted type.
   *
   * @return converted type
   */
  public FunctionName convertFunctionName() {
    String type = convertedType.toString().toLowerCase(Locale.ROOT);
    if (CONVERTED_TYPE_FUNCTION_NAME_MAP.containsKey(type)) {
      return CONVERTED_TYPE_FUNCTION_NAME_MAP.get(type);
    } else {
      throw new IllegalStateException("unsupported cast type: " + type);
    }
  }

  @Override
  public List<? extends Node> getChild() {
    return Collections.singletonList(expression);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitCast(this, context);
  }
}
