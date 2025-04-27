/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.datetime;

import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.PPLReturnTypes;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.expression.function.ImplementorUDF;

/**
 * Implementation of DAYNAME and MONTHNAME functions. It returns the names as strings of day (e.g.
 * Monday) or month (e.g. January) in local language.
 *
 * <p>Signature:
 *
 * <ul>
 *   <li>DATE/TIMESTAMP/STRING -> STRING
 * </ul>
 */
public class PeriodNameFunction extends ImplementorUDF {
  public PeriodNameFunction(TimeUnit periodUnit) {
    super(new PeriodNameFunctionImplementor(periodUnit), NullPolicy.ANY);
    if (!periodUnit.equals(TimeUnit.DAY) && !periodUnit.equals(TimeUnit.MONTH)) {
      throw new IllegalArgumentException(
          "PeriodName is only implemented for DAY and MONTH, but got: " + periodUnit);
    }
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return PPLReturnTypes.STRING_FORCE_NULLABLE;
  }

  public static class PeriodNameFunctionImplementor implements NotNullImplementor {
    private final TimeUnit periodUnit;

    public PeriodNameFunctionImplementor(TimeUnit periodUnit) {
      super();
      this.periodUnit = periodUnit;
    }

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      SqlTypeName dateType =
          OpenSearchTypeFactory.convertRelDataTypeToSqlTypeName(
              call.getOperands().getFirst().getType());
      return Expressions.call(
          PeriodNameFunctionImplementor.class,
          "name",
          translatedOperands.getFirst(),
          Expressions.constant(periodUnit));
    }

    public static String name(String date, TimeUnit periodUnit) {
      LocalDate localDate = new ExprDateValue(date).dateValue();

      if (periodUnit.equals(TimeUnit.MONTH)) {
        return localDate.getMonth().getDisplayName(TextStyle.FULL, Locale.getDefault());
      } else if (periodUnit.equals(TimeUnit.DAY)) {
        return localDate.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.getDefault());
      } else {
        throw new IllegalArgumentException(
            "PeriodName is only implemented for DAY and MONTH, but got: " + periodUnit);
      }
    }
  }
}
