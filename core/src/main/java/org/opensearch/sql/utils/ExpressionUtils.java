/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.expression.Expression;

/** Utils for {@link Expression}. */
@UtilityClass
public class ExpressionUtils {

  /** Format the list of {@link Expression}. */
  public static String format(List<Expression> expressionList) {
    return expressionList.stream().map(Expression::toString).collect(Collectors.joining(","));
  }
}
