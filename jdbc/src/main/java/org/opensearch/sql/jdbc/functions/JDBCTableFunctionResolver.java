/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.functions;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.jdbc.parser.PropertiesParser;

/**
 * JDBC datasource defined {@link FunctionResolver}.
 */
@RequiredArgsConstructor
public class JDBCTableFunctionResolver implements FunctionResolver {

  public static final FunctionName JDBC_FUNCTION_NAME = FunctionName.of("jdbc");

  @VisibleForTesting
  public static final FunctionSignature JDBC_FUNCTION_SIGNATURE =
      new FunctionSignature(JDBC_FUNCTION_NAME, List.of(STRING));

  private final DataSourceMetadata dataSourceMetadata;

  private final PropertiesParser propertiesParser;

  @Override
  public Pair<FunctionSignature, FunctionBuilder> resolve(FunctionSignature functionSignature) {
    FunctionBuilder functionBuilder =
        (properties, arguments) -> {
          try {
            String sqlQuery = arguments.get(0).valueOf().stringValue();
            return new JDBCFunction(
                JDBC_FUNCTION_NAME,
                sqlQuery,
                propertiesParser.parse(dataSourceMetadata.getProperties()));
          } catch (ExpressionEvaluationException e) {
            throw new SyntaxCheckException(
                String.format(
                    Locale.ROOT,
                    "SQL statement is required. For example %s.jdbc('select * from table')",
                    dataSourceMetadata.getName()));
          }
        };
    return Pair.of(JDBC_FUNCTION_SIGNATURE, functionBuilder);
  }

  @Override
  public FunctionName getFunctionName() {
    return JDBC_FUNCTION_NAME;
  }
}
