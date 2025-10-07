/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.SparkSqlDialect;

/**
 * Custom Spark SQL dialect that extends Calcite's SparkSqlDialect to handle OpenSearch-specific
 * function translations. This dialect ensures that functions are translated to their correct Spark
 * SQL equivalents.
 */
public class OpenSearchSparkSqlDialect extends SparkSqlDialect {

  /** Singleton instance of the OpenSearch Spark SQL dialect. */
  public static final OpenSearchSparkSqlDialect DEFAULT = new OpenSearchSparkSqlDialect();

  private static final Map<String, String> CALCITE_TO_SPARK_MAPPING =
      ImmutableMap.of(
          "ARG_MIN", "MIN_BY",
          "ARG_MAX", "MAX_BY");

  private OpenSearchSparkSqlDialect() {
    super(DEFAULT_CONTEXT);
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String operatorName = call.getOperator().getName();

    // Replace Calcite specific functions with their Spark SQL equivalents
    if (CALCITE_TO_SPARK_MAPPING.containsKey(operatorName)) {
      unparseFunction(
          writer, call, CALCITE_TO_SPARK_MAPPING.get(operatorName), leftPrec, rightPrec);
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  private void unparseFunction(
      SqlWriter writer, SqlCall call, String functionName, int leftPrec, int rightPrec) {
    writer.keyword(functionName);
    final SqlWriter.Frame frame = writer.startList("(", ")");
    for (int i = 0; i < call.operandCount(); i++) {
      if (i > 0) {
        writer.sep(",");
      }
      call.operand(i).unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }
}
