/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.model;

/** The definition of the missing value. */
public class ExprMissingValue implements ExprValue {
  @Override
  public ExprValueKind kind() {
    return ExprValueKind.MISSING_VALUE;
  }
}
