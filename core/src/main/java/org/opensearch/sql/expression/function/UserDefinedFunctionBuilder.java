/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.Collections;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

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

  /**
 * Provides operand metadata describing the UDF's expected arguments for validation and type checking.
 *
 * Implementations may return {@code null} if the function does not expose operand metadata.
 *
 * @return the UDFOperandMetadata for this function, or {@code null} when no metadata is supplied
 */
UDFOperandMetadata getOperandMetadata();

  /**
   * Specifies the SQL kind to assign to the constructed user-defined function.
   *
   * @return the SqlKind to use when creating the SqlUserDefinedFunction, defaults to {@link SqlKind#OTHER_FUNCTION}.
   */
  default SqlKind getKind() {
    return SqlKind.OTHER_FUNCTION;
  }

  /**
   * Create a SqlUserDefinedFunction for the given function name using the builder's default determinism.
   *
   * @param functionName the name of the function as it will appear in SQL
   * @return the SqlUserDefinedFunction for the specified name using the builder's default determinism
   */
  default SqlUserDefinedFunction toUDF(String functionName) {
    return toUDF(functionName, true);
  }

  /**
   * Create a Calcite SqlUserDefinedFunction for the given UDF name with the specified determinism.
   *
   * The produced function uses this builder's return type inference, operand metadata, and implementation.
   *
   * @param functionName the UDF name to register
   * @param isDeterministic whether the created function should be treated as deterministic
   * @return a configured SqlUserDefinedFunction instance
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

      /**
       * Indicates the UDF does not expose a SQL identifier and should be rendered using function syntax or a keyword.
       *
       * @return {@code null} to signal there is no identifier and to trigger function-syntax/keyword unparsing
       */
      @Override
      public SqlIdentifier getSqlIdentifier() {
        // to avoid convert to sql dialog as identifier, use keyword instead
        // check the code SqlUtil.unparseFunctionSyntax()
        return null;
      }

      /**
       * Provide the allowed operand count range for this function.
       *
       * @return the SqlOperandCountRange that specifies the minimum and maximum number of operands permitted for this function
       */
      @Override
      public SqlOperandCountRange getOperandCountRange() {
        return getOperandMetadata().getOperandCountRange();
      }
    };
  }
}