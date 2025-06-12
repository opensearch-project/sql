/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.util.Collections;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
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

  UDFOperandMetadata getOperandMetadata();

  default SqlUserDefinedFunction toUDF(String functionName) {
    SqlIdentifier udfLtrimIdentifier =
        new SqlIdentifier(Collections.singletonList(functionName), null, SqlParserPos.ZERO, null);
    return new SqlUserDefinedFunction(
        udfLtrimIdentifier,
        SqlKind.OTHER_FUNCTION,
        getReturnTypeInference(),
        InferTypes.ANY_NULLABLE,
        getOperandMetadata(),
        getFunction());
  }
}
