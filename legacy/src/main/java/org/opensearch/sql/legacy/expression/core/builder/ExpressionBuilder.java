/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.core.builder;

import java.util.List;
import org.opensearch.sql.legacy.expression.core.Expression;

/** The definition of the {@link Expression} builder. */
public interface ExpressionBuilder {
  Expression build(List<Expression> expressionList);
}
