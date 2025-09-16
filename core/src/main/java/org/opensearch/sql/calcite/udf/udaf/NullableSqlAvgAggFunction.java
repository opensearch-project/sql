/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.udf.udaf;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.Optionality;

public class NullableSqlAvgAggFunction extends SqlAggFunction {

  // ~ Constructors -----------------------------------------------------------

  /** Creates a NullableSqlAvgAggFunction. */
  public NullableSqlAvgAggFunction(SqlKind kind) {
    this(kind.name(), kind);
  }

  NullableSqlAvgAggFunction(String name, SqlKind kind) {
    super(
        name,
        null,
        kind,
        ReturnTypes.AVG_AGG_FUNCTION.andThen(SqlTypeTransforms.FORCE_NULLABLE), // modified here
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC,
        false,
        false,
        Optionality.FORBIDDEN);
    checkArgument(SqlKind.AVG_AGG_FUNCTIONS.contains(kind), "unsupported sql kind");
  }

  // ~ Methods ----------------------------------------------------------------

  /**
   * Returns the specific function, e.g. AVG or STDDEV_POP.
   *
   * @return Subtype
   */
  @Deprecated // to be removed before 2.0
  public SqlAvgAggFunction.Subtype getSubtype() {
    return SqlAvgAggFunction.Subtype.valueOf(kind.name());
  }

  /** Sub-type of aggregate function. */
  @Deprecated // to be removed before 2.0
  public enum Subtype {
    AVG,
    STDDEV_POP,
    STDDEV_SAMP,
    VAR_POP,
    VAR_SAMP
  }
}
