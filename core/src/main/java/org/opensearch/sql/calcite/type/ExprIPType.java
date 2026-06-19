/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_IP;

import org.apache.calcite.rel.type.RelDataTypeFactoryImpl.JavaType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprIpValue;

public class ExprIPType extends ExprJavaType {
  public ExprIPType(OpenSearchTypeFactory typeFactory) {
    super(typeFactory, EXPR_IP, ExprIpValue.class);
  }

  private ExprIPType(JavaType type) {
    super(EXPR_IP, type);
  }

  @Override
  protected ExprJavaType cloneWith(JavaType inner) {
    return new ExprIPType(inner);
  }
}
