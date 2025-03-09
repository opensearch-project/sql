/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Optionality;

/**
 * This class is used to support the legacy COUNT(*) function. It extends SqlAggFunction and
 * overrides the deriveType method to return INTEGER type.
 */
public class LegacyPPLCountAggFunction extends SqlAggFunction {

  public LegacyPPLCountAggFunction() {
    super(
        "COUNT",
        null,
        SqlKind.COUNT,
        ReturnTypes.INTEGER, // Change return type to INTEGER
        null,
        OperandTypes.ONE_OR_MORE,
        SqlFunctionCategory.NUMERIC,
        false,
        false,
        Optionality.FORBIDDEN);
  }

  @Override
  public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
    return validator.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
  }
}
