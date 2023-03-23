/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.jdbc.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.jdbc.functions.JDBCTableFunctionResolver.JDBC_FUNCTION_NAME;
import static org.opensearch.sql.jdbc.functions.JDBCTableFunctionResolver.JDBC_FUNCTION_SIGNATURE;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionImplementation;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.jdbc.parser.PropertiesParser;

@ExtendWith(MockitoExtension.class)
class JDBCTableFunctionResolverTest {
  @Mock
  private DataSourceMetadata metadata;

  @Mock
  private PropertiesParser parser;

  @Mock
  private FunctionSignature functionSignature;

  @Mock
  private FunctionProperties properties;

  @Test
  public void getFunctionName() {
    JDBCTableFunctionResolver resolver =
        new JDBCTableFunctionResolver(metadata, parser);
    assertEquals(JDBC_FUNCTION_NAME, resolver.getFunctionName());
  }

  @Test
  public void resolve() {
    Pair<FunctionSignature, FunctionBuilder> functionBuilderPair =
        new JDBCTableFunctionResolver(metadata, parser).resolve(functionSignature);

    assertEquals(JDBC_FUNCTION_SIGNATURE, functionBuilderPair.getLeft());
    assertNotNull(functionBuilderPair.getRight());
  }

  @Test
  public void functionBuilder() {
    when(metadata.getProperties()).thenReturn(ImmutableMap.of());

    FunctionBuilder functionBuilder =
        new JDBCTableFunctionResolver(metadata, parser).resolve(functionSignature).getRight();
    FunctionImplementation implementation = functionBuilder
        .apply(properties, Collections.singletonList(literal("select * from table")));
    assertTrue(implementation instanceof JDBCFunction);
  }

  @Test
  public void functionBuilderSyntaxCheckException() {
    when(metadata.getName()).thenReturn("mysource");
    FunctionBuilder functionBuilder =
        new JDBCTableFunctionResolver(metadata, parser).resolve(functionSignature).getRight();

    SyntaxCheckException exception =
        assertThrows(
            SyntaxCheckException.class,
            () -> functionBuilder.apply(properties, Collections.singletonList(literal(1L))));
    assertEquals(
        "SQL statement is required. For example mysource.jdbc('select * from table')",
        exception.getMessage());
  }
}
