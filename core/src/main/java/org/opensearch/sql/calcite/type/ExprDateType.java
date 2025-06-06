/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;

public class ExprDateType extends ExprSqlType {
  public ExprDateType(OpenSearchTypeFactory typeFactory) {
    super(typeFactory, EXPR_DATE, SqlTypeName.VARCHAR);
  }
}
