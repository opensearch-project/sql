/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import static org.opensearch.sql.config.TestConfig.BOOL_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.BOOL_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.DOUBLE_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.DOUBLE_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.INT_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.INT_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.STRING_TYPE_MISSING_VALUE_FIELD;
import static org.opensearch.sql.config.TestConfig.STRING_TYPE_NULL_VALUE_FIELD;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.floatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.missingValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionProperties;

public class ExpressionTestBase {

  protected FunctionProperties functionProperties = new FunctionProperties();

  protected Environment<Expression, ExprType> typeEnv;

  protected static Environment<Expression, ExprValue> valueEnv() {
    return var -> {
      if (var instanceof ReferenceExpression) {
        switch (((ReferenceExpression) var).getAttr()) {
          case "integer_value":
            return integerValue(1);
          case "long_value":
            return longValue(1L);
          case "float_value":
            return floatValue(1f);
          case "double_value":
            return doubleValue(1d);
          case "boolean_value":
            return booleanValue(true);
          case "string_value":
            return stringValue("str");
          case "struct_value":
            return tupleValue(ImmutableMap.of("str", 1));
          case "array_value":
            return collectionValue(ImmutableList.of(1));
          case BOOL_TYPE_NULL_VALUE_FIELD:
          case INT_TYPE_NULL_VALUE_FIELD:
          case DOUBLE_TYPE_NULL_VALUE_FIELD:
          case STRING_TYPE_NULL_VALUE_FIELD:
            return nullValue();
          case INT_TYPE_MISSING_VALUE_FIELD:
          case BOOL_TYPE_MISSING_VALUE_FIELD:
          case DOUBLE_TYPE_MISSING_VALUE_FIELD:
          case STRING_TYPE_MISSING_VALUE_FIELD:
            return missingValue();
          default:
            throw new IllegalArgumentException("undefined reference");
        }
      } else {
        throw new IllegalArgumentException("var must be ReferenceExpression");
      }
    };
  }
}
