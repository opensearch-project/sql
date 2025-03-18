/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf;

import java.time.LocalDate;
import java.time.ZoneOffset;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.planner.physical.collector.Rounding.DateTimeUnit;

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

    SqlTypeName sqlTypeName = SqlTypeName.valueOf((String) args[1]);
    Integer interval = (Integer) args[2];
    DateTimeUnit dateTimeUnit = DateTimeUnit.resolve((String) args[3]);

    switch (sqlTypeName) {
      case SqlTypeName.DATE:
        LocalDate date = LocalDate.ofEpochDay(((Integer) args[0]).longValue());
        long dateEpochValue =
            dateTimeUnit.round(
                date.atStartOfDay().atZone(ZoneOffset.UTC).toInstant().toEpochMilli(), interval);
        return SqlFunctions.timestampToDate(dateEpochValue);
      case SqlTypeName.TIME:
        /*
         * Follow current logic to ignore time frame greater than hour because TIME type like '17:59:59.99' doesn't have day, month, year, etc.
         * See @org.opensearch.sql.planner.physical.collector.TimeRounding
         */
        if (dateTimeUnit.getId() > 4) {
          throw new IllegalArgumentException(
              String.format("Unable to set span unit %s for TIME type", dateTimeUnit.getName()));
        }
        long timeEpochValue = dateTimeUnit.round(((Integer) args[0]).longValue(), interval);
        return SqlFunctions.time(timeEpochValue);
      case SqlTypeName.TIMESTAMP:
        return dateTimeUnit.round((long) args[0], interval);
      default:
        throw new IllegalArgumentException("Unsupported time based column in Span function");
    }
  }
}
