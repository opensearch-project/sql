/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class ExprBinaryType extends ExprSqlType {
  public ExprBinaryType(OpenSearchTypeFactory typeFactory) {
    super(typeFactory, ExprUDT.EXPR_BINARY, SqlTypeName.VARCHAR);
  }
}
