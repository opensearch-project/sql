/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP;

import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public class ExprTimeStampType extends ExprSqlType {
  public ExprTimeStampType(OpenSearchTypeFactory typeFactory) {
    super(typeFactory, EXPR_TIMESTAMP, SqlTypeName.VARCHAR);
  }

  private ExprTimeStampType(BasicSqlType type) {
    super(EXPR_TIMESTAMP, type);
  }

  @Override
  protected ExprSqlType cloneWith(BasicSqlType inner) {
    return new ExprTimeStampType(inner);
  }
}
