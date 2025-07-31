/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.Collections;
import java.util.function.Supplier;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The interface helps to construct a SqlUserDefinedFunction
 *
 * <p>1. getFunction - returns the implementation of the UDF
 *
 * <p>2. getReturnTypeInference - returns the return type of the UDF
 *
 * <p>3. getOperandMetadata - returns the operand metadata of the UDF. This is for checking the
 * operand when validation, default null without checking.
 */
public interface UserDefinedFunctionBuilder {

  ImplementableFunction getFunction();

  SqlReturnTypeInference getReturnTypeInference();

  UDFOperandMetadata getOperandMetadata();

  default SqlKind getKind() {
    return SqlKind.OTHER_FUNCTION;
  }

  default SqlSyntax getSqlSyntax() {
    return SqlSyntax.FUNCTION;
  }

  default Supplier<SqlOperator> getReverse() {
    return null;
  }

  default SqlUserDefinedFunction toUDF(String functionName) {
    return toUDF(functionName, true);
  }

  /**
   * In some rare cases, we need to call out the UDF to be not deterministic to avoid Volcano
   * planner over-optimization. For example, we don't need ReduceExpressionsRule to optimize
   * relevance query UDF.
   *
   * @param functionName UDF name to be registered
   * @param isDeterministic Specified isDeterministic flag
   * @return Calcite SqlUserDefinedFunction
   */
  default SqlUserDefinedFunction toUDF(String functionName, boolean isDeterministic) {
    SqlIdentifier udfLtrimIdentifier =
        new SqlIdentifier(Collections.singletonList(functionName), null, SqlParserPos.ZERO, null);
    return new SqlUserDefinedFunction(
        udfLtrimIdentifier,
        getKind(),
        getReturnTypeInference(),
        InferTypes.ANY_NULLABLE,
        getOperandMetadata(),
        getFunction()) {
      @Override
      public boolean isDeterministic() {
        return isDeterministic;
      }

      @Override
      public @Nullable SqlOperator reverse() {
        if (getReverse() == null) {
          return null;
        }
        return getReverse().get();
      }

      @Override
      public SqlSyntax getSyntax() {
        return getSqlSyntax();
      }
    };
  }
}
