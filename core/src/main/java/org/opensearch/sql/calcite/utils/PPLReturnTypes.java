/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.opensearch.sql.data.type.ExprCoreType;

/**
 * Return types used in PPL. This class complements the {@link
 * org.apache.calcite.sql.type.ReturnTypes} class.
 */
public final class PPLReturnTypes {
  private PPLReturnTypes() {}

  public static final SqlReturnTypeInference DATE_FORCE_NULLABLE =
      ReturnTypes.explicit(UserDefinedFunctionUtils.NULLABLE_DATE_UDT);
  public static final SqlReturnTypeInference TIME_FORCE_NULLABLE =
      ReturnTypes.explicit(UserDefinedFunctionUtils.NULLABLE_TIME_UDT);
  public static final SqlReturnTypeInference TIMESTAMP_FORCE_NULLABLE =
      ReturnTypes.explicit(UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT);
  public static SqlReturnTypeInference INTEGER_FORCE_NULLABLE =
      ReturnTypes.INTEGER.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  public static SqlReturnTypeInference STRING_FORCE_NULLABLE =
      ReturnTypes.VARCHAR.andThen(SqlTypeTransforms.FORCE_NULLABLE);
  public static SqlReturnTypeInference TIME_APPLY_RETURN_TYPE =
      opBinding -> {
        RelDataType temporalType = opBinding.getOperandType(0);
        if (ExprCoreType.TIME.equals(
            OpenSearchTypeFactory.convertRelDataTypeToExprType(temporalType))) {
          return UserDefinedFunctionUtils.NULLABLE_TIME_UDT;
        }
        return UserDefinedFunctionUtils.NULLABLE_TIMESTAMP_UDT;
      };
}
