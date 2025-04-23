/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.convertSqlTypeNameToExprType;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.*;
import static org.opensearch.sql.data.model.ExprValueUtils.fromObjectValue;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.*;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.ImplementorUDF;

public class UnixTimestampFunction extends ImplementorUDF {
  public UnixTimestampFunction() {
    super(new UnixTimestampImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.DOUBLE_FORCE_NULLABLE;
  }

  public static class UnixTimestampImplementor implements NotNullImplementor {
    @Override
    public Expression implement(
        RexToLixTranslator rexToLixTranslator, RexCall rexCall, List<Expression> list) {
      List<Expression> newList = addTypeAndContext(list, rexCall, rexToLixTranslator.getRoot());
      return Expressions.call(UnixTimestampFunction.class, "unixTimestamp", newList);
    }
  }

  public static Object unixTimestamp(DataContext propertyContext) {
    FunctionProperties restored = restoreFunctionProperties(propertyContext);
    return unixTimeStamp(restored.getQueryStartClock()).doubleValue();
  }

  public static Object unixTimestamp(
      Object timestamp, SqlTypeName timestampType, DataContext propertyContext) {
    ExprValue candidate = fromObjectValue(timestamp, convertSqlTypeNameToExprType(timestampType));
    return unixTimeStampOf(candidate).doubleValue();
  }
}
