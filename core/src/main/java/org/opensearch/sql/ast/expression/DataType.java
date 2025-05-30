/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.expression;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;

/** The DataType definition in AST. Question, could we use {@link ExprCoreType} directly in AST? */
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
  INTERVAL(ExprCoreType.INTERVAL),

  // the decimal DataType is only used for building decimal literal,
  // so it still maps to double core type until we support decimal type.
  // ref https://github.com/opensearch-project/sql/issues/3619
  DECIMAL(ExprCoreType.DOUBLE),

  IP(ExprCoreType.IP);

  @Getter private final ExprCoreType coreType;
}
