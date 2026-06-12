/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIME;

import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public class ExprTimeType extends ExprSqlType {
  public ExprTimeType(OpenSearchTypeFactory typeFactory) {
    super(typeFactory, EXPR_TIME, SqlTypeName.VARCHAR);
  }

  private ExprTimeType(BasicSqlType type) {
    super(EXPR_TIME, type);
  }

  @Override
  protected ExprSqlType cloneWith(BasicSqlType inner) {
    return new ExprTimeType(inner);
  }
}
