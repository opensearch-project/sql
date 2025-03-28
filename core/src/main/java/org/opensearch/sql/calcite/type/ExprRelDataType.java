/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import java.lang.reflect.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.opensearch.sql.data.type.ExprType;

public interface ExprRelDataType extends RelDataType, RelDataTypeFamily {
  ExprType getExprType();

  Type getJavaType();
}
