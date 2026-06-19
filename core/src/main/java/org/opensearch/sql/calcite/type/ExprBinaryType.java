/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_BINARY;

import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public class ExprBinaryType extends ExprSqlType {
  public ExprBinaryType(OpenSearchTypeFactory typeFactory) {
    super(typeFactory, EXPR_BINARY, SqlTypeName.VARCHAR);
  }

  private ExprBinaryType(BasicSqlType type) {
    super(EXPR_BINARY, type);
  }

  @Override
  protected ExprSqlType cloneWith(BasicSqlType inner) {
    return new ExprBinaryType(inner);
  }
}
