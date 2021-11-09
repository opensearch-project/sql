/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.storage.script.filter.lucene;

import static org.opensearch.sql.opensearch.data.type.OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.Function;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.data.model.ExprBooleanValue;
import org.opensearch.sql.data.model.ExprByteValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.FunctionName;

/**
 * Lucene query abstraction that builds Lucene query from function expression.
 */
public abstract class LuceneQuery {

  /**
   * Check if function expression supported by current Lucene query.
   * Default behavior is that report supported if:
   *  1. Left is a reference
   *  2. Right side is a literal
   *
   * @param func    function
   * @return        return true if supported, otherwise false.
   */
  public boolean canSupport(FunctionExpression func) {
    return (func.getArguments().size() == 2)
        && (func.getArguments().get(0) instanceof ReferenceExpression)
        && (func.getArguments().get(1) instanceof LiteralExpression
        || literalExpressionWrappedByCast(func))
        || isMultiParameterQuery(func);
  }

  /**
   * Check if the function expression has multiple named argument expressions as the parameters.
   *
   * @param func      function
   * @return          return true if the expression is a multi-parameter function.
   */
  private boolean isMultiParameterQuery(FunctionExpression func) {
    for (Expression expr : func.getArguments()) {
      if (!(expr instanceof NamedArgumentExpression)) {
        return false;
      }
    }
    return true;
  }

  private boolean literalExpressionWrappedByCast(FunctionExpression func) {
    if (func.getArguments().get(1) instanceof FunctionExpression) {
      return castMap.containsKey(((FunctionExpression) func.getArguments().get(1))
          .getFunctionName());
    }
    return false;
  }

  /**
   * Build Lucene query from function expression.
   * The cast function is converted to literal expressions before generating DSL.
   *
   * @param func  function
   * @return      query
   */
  public QueryBuilder build(FunctionExpression func) {
    ReferenceExpression ref = (ReferenceExpression) func.getArguments().get(0);
    Expression expr = func.getArguments().get(1);
    ExprValue literalValue = expr instanceof LiteralExpression ? expr
        .valueOf(null) : cast((FunctionExpression) expr);
    return doBuild(ref.getAttr(), ref.type(), literalValue);
  }

  private ExprValue cast(FunctionExpression castFunction) {
    return castMap.get(castFunction.getFunctionName()).apply(
        (LiteralExpression) castFunction.getArguments().get(0));
  }

  /**
   * Type converting map.
   */
  private final Map<FunctionName, Function<LiteralExpression, ExprValue>> castMap = ImmutableMap
      .<FunctionName, Function<LiteralExpression, ExprValue>>builder()
      .put(BuiltinFunctionName.CAST_TO_STRING.getName(), expr -> {
        if (!expr.type().equals(ExprCoreType.STRING)) {
          return new ExprStringValue(String.valueOf(expr.valueOf(null).value()));
        } else {
          return expr.valueOf(null);
        }
      })
      .put(BuiltinFunctionName.CAST_TO_BYTE.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprByteValue(expr.valueOf(null).byteValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprByteValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprByteValue(Byte.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_SHORT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprShortValue(expr.valueOf(null).shortValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprShortValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprShortValue(Short.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_INT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprIntegerValue(expr.valueOf(null).integerValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprIntegerValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprIntegerValue(Integer.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_LONG.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprLongValue(expr.valueOf(null).longValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprLongValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprLongValue(Long.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_FLOAT.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprFloatValue(expr.valueOf(null).floatValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprFloatValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprFloatValue(Float.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DOUBLE.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return new ExprDoubleValue(expr.valueOf(null).doubleValue());
        } else if (expr.type().equals(ExprCoreType.BOOLEAN)) {
          return new ExprDoubleValue(expr.valueOf(null).booleanValue() ? 1 : 0);
        } else {
          return new ExprDoubleValue(Double.valueOf(expr.valueOf(null).stringValue()));
        }
      })
      .put(BuiltinFunctionName.CAST_TO_BOOLEAN.getName(), expr -> {
        if (ExprCoreType.numberTypes().contains(expr.type())) {
          return expr.valueOf(null).doubleValue() == 1
              ? ExprBooleanValue.of(true) : ExprBooleanValue.of(false);
        } else if (expr.type().equals(ExprCoreType.STRING)) {
          return ExprBooleanValue.of(Boolean.valueOf(expr.valueOf(null).stringValue()));
        } else {
          return expr.valueOf(null);
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DATE.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprDateValue(expr.valueOf(null).stringValue());
        } else {
          return new ExprDateValue(expr.valueOf(null).dateValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_TIME.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprTimeValue(expr.valueOf(null).stringValue());
        } else {
          return new ExprTimeValue(expr.valueOf(null).timeValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_DATETIME.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprDatetimeValue(expr.valueOf(null).stringValue());
        } else {
          return new ExprDatetimeValue(expr.valueOf(null).datetimeValue());
        }
      })
      .put(BuiltinFunctionName.CAST_TO_TIMESTAMP.getName(), expr -> {
        if (expr.type().equals(ExprCoreType.STRING)) {
          return new ExprTimestampValue(expr.valueOf(null).stringValue());
        } else {
          return new ExprTimestampValue(expr.valueOf(null).timestampValue());
        }
      })
      .build();

  /**
   * Build method that subclass implements by default which is to build query
   * from reference and literal in function arguments.
   *
   * @param fieldName   field name
   * @param fieldType   field type
   * @param literal     field value literal
   * @return            query
   */
  protected QueryBuilder doBuild(String fieldName, ExprType fieldType, ExprValue literal) {
    throw new UnsupportedOperationException(
        "Subclass doesn't implement this and build method either");
  }

  /**
   * Convert multi-field text field name to its inner keyword field. The limitation and assumption
   * is that the keyword field name is always "keyword" which is true by default.
   *
   * @param fieldName   field name
   * @param fieldType   field type
   * @return            keyword field name for multi-field, otherwise original field name returned
   */
  protected String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType == OPENSEARCH_TEXT_KEYWORD) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }

}
