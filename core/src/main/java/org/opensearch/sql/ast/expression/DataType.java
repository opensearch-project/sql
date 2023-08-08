/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;

/** The DataType defintion in AST. Question, could we use {@link ExprCoreType} directly in AST? */
@RequiredArgsConstructor
public enum DataType {
  TYPE_ERROR(ExprCoreType.UNKNOWN),
  NULL(ExprCoreType.UNDEFINED),

  INTEGER(ExprCoreType.INTEGER),
  LONG(ExprCoreType.LONG),
  SHORT(ExprCoreType.SHORT),
  FLOAT(ExprCoreType.FLOAT),
  DOUBLE(ExprCoreType.DOUBLE),
  STRING(ExprCoreType.STRING),
  BOOLEAN(ExprCoreType.BOOLEAN),

  DATE(ExprCoreType.DATE),
  TIME(ExprCoreType.TIME),
  TIMESTAMP(ExprCoreType.TIMESTAMP),
  INTERVAL(ExprCoreType.INTERVAL);

  @Getter private final ExprCoreType coreType;
}
