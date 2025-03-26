/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.opensearch.sql.data.type.ExprType;

public interface ExprUserDefinedType extends RelDataType, RelDataTypeFamily {
  ExprType getExprType();
}
