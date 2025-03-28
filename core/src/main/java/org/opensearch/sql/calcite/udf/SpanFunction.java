/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf;

import org.apache.calcite.linq4j.function.Strict;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.planner.physical.collector.Rounding;
import org.opensearch.sql.planner.physical.collector.Rounding.DateRounding;
import org.opensearch.sql.planner.physical.collector.Rounding.TimeRounding;
import org.opensearch.sql.planner.physical.collector.Rounding.TimestampRounding;

/**
 * Implement a customized UDF for span function because calcite doesn't have handy function to
 * support all rounding logic for original OpenSearch time based column
 *
 * <ol>
 *   <li>The tumble function usage of `group by tumble(field)` only works for stream SQL
 *   <li>The tumble function usage of `table(tumble(table t1, descriptor field, interval))` only
 *       works for TableFunctionScan logical plan
 *   <li>Builtin function `FLOOR(date field to day)` only works for standard single interval like 1
 *       day, 1 month, 1 hour
 * </ol>
 *
 * TODO: Refactor SpanFunction with customized implementor for better reusability and efficiency
 */
public class SpanFunction implements UserDefinedFunction {

  @Override
  @Strict // annotation allows pre-checking the input nullability before jumping to eval()
  public Object eval(Object... args) {
    if (args.length < 4) {
      throw new IllegalArgumentException("Span function requires at least 4 parameters");
    }

    String value = (String) args[0];
    String type = (String) args[1];
    Integer interval = (Integer) args[2];
    String unitName = (String) args[3];

    ExprValue exprInterval = ExprValueUtils.fromObjectValue(interval, ExprCoreType.INTEGER);

    ExprCoreType exprCoreType = ExprUDT.valueOf(type).getExprCoreType();

    ExprValue exprValue = ExprValueUtils.fromObjectValue(value, exprCoreType);

    Rounding<?> rounding =
        (switch (exprCoreType) {
          case ExprCoreType.DATE -> new DateRounding(exprInterval, unitName);
          case ExprCoreType.TIME -> new TimeRounding(exprInterval, unitName);
          case ExprCoreType.TIMESTAMP -> new TimestampRounding(exprInterval, unitName);
          default -> throw new IllegalStateException("Unexpected value: " + exprCoreType);
        });
    return rounding.round(exprValue).valueForCalcite();
  }
}
