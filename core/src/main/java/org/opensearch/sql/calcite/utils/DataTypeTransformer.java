/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.opensearch.sql.ast.expression.SpanUnit.DAY;
import static org.opensearch.sql.ast.expression.SpanUnit.HOUR;
import static org.opensearch.sql.ast.expression.SpanUnit.MILLISECOND;
import static org.opensearch.sql.ast.expression.SpanUnit.MINUTE;
import static org.opensearch.sql.ast.expression.SpanUnit.MONTH;
import static org.opensearch.sql.ast.expression.SpanUnit.NONE;
import static org.opensearch.sql.ast.expression.SpanUnit.QUARTER;
import static org.opensearch.sql.ast.expression.SpanUnit.SECOND;
import static org.opensearch.sql.ast.expression.SpanUnit.WEEK;
import static org.opensearch.sql.ast.expression.SpanUnit.YEAR;

import org.opensearch.sql.ast.expression.SpanUnit;

public interface DataTypeTransformer {

  static String translate(SpanUnit unit) {
    switch (unit) {
      case UNKNOWN:
      case NONE:
        return NONE.name();
      case MILLISECOND:
      case MS:
        return MILLISECOND.name();
      case SECOND:
      case S:
        return SECOND.name();
      case MINUTE:
      case m:
        return MINUTE.name();
      case HOUR:
      case H:
        return HOUR.name();
      case DAY:
      case D:
        return DAY.name();
      case WEEK:
      case W:
        return WEEK.name();
      case MONTH:
      case M:
        return MONTH.name();
      case QUARTER:
      case Q:
        return QUARTER.name();
      case YEAR:
      case Y:
        return YEAR.name();
    }
    return "";
  }
}
