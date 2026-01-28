/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.validate;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlDelegatingConformance;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;

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
          "ARG_MAX", "MAX_BY",
          "SAFE_CAST", "TRY_CAST");

  private static final Map<String, String> CALL_SEPARATOR = ImmutableMap.of("SAFE_CAST", "AS");

  private OpenSearchSparkSqlDialect() {
    super(DEFAULT_CONTEXT);
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    String operatorName = call.getOperator().getName();

    // Replace Calcite specific functions with their Spark SQL equivalents
    if (CALCITE_TO_SPARK_MAPPING.containsKey(operatorName)) {
      unparseFunction(
          writer,
          call,
          CALCITE_TO_SPARK_MAPPING.get(operatorName),
          leftPrec,
          rightPrec,
          CALL_SEPARATOR.getOrDefault(operatorName, ","));
    } else {
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

  @Override
  public @Nullable SqlNode getCastSpec(RelDataType type) {
    // ExprIPType has sql type name OTHER, which can not be handled by spark dialect
    if (OpenSearchTypeUtil.isIp(type)) {
      return new SqlDataTypeSpec(
          // It will use SqlTypeName.OTHER by type.getSqlTypeName() as OTHER is "borrowed" to
          // represent IP type (see also: PplTypeCoercionRule.java)
          new SqlAlienSystemTypeNameSpec("IP", type.getSqlTypeName(), SqlParserPos.ZERO),
          SqlParserPos.ZERO);
    }
    return super.getCastSpec(type);
  }

  private void unparseFunction(
      SqlWriter writer,
      SqlCall call,
      String functionName,
      int leftPrec,
      int rightPrec,
      String separator) {
    writer.print(functionName);
    final SqlWriter.Frame frame = writer.startList("(", ")");
    for (int i = 0; i < call.operandCount(); i++) {
      if (i > 0) {
        writer.sep(separator);
      }
      call.operand(i).unparse(writer, 0, rightPrec);
    }
    writer.endList(frame);
  }

  @Override
  public SqlConformance getConformance() {
    return new SqlDelegatingConformance(super.getConformance()) {
      @Override
      public boolean isLiberal() {
        // This allows SQL feature LEFT ANTI JOIN & LEFT SEMI JOIN
        return true;
      }
    };
  }
}
