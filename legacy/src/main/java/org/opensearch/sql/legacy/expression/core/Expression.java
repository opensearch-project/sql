/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.core;

import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.expression.model.ExprValue;

/** The definition of the Expression. */
public interface Expression {
  /**
   * Evaluate the result on the BindingTuple context.
   *
   * @param tuple BindingTuple
   * @return ExprValue
   */
  ExprValue valueOf(BindingTuple tuple);
}
