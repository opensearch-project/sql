/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.data.model.ExprIpValue;

public class ExprIPType extends ExprJavaType {
  public ExprIPType(OpenSearchTypeFactory typeFactory) {
    super(typeFactory, ExprUDT.EXPR_IP, ExprIpValue.class);
  }
}
