/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.functions;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.function.FunctionBuilder;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.implementation.SqlFunctionImplementation;
import org.opensearch.sql.spark.functions.resolver.SqlTableFunctionResolver;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.spark.storage.SparkMetricTable;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

@ExtendWith(MockitoExtension.class)
public class SqlTableFunctionResolverTest {
    @Mock
    private SparkClient client;

    @Mock
    private FunctionProperties functionProperties;

    @Test
    void testResolve() {
        SqlTableFunctionResolver sqlTableFunctionResolver
                = new SqlTableFunctionResolver(client);
        FunctionName functionName = FunctionName.of("sql");
        List<Expression> expressions
                = List.of(DSL.namedArgument("query", DSL.literal("select 1")));
        FunctionSignature functionSignature = new FunctionSignature(functionName, expressions
                .stream().map(Expression::type).collect(Collectors.toList()));
        Pair<FunctionSignature, FunctionBuilder> resolution
                = sqlTableFunctionResolver.resolve(functionSignature);
        assertEquals(functionName, resolution.getKey().getFunctionName());
        assertEquals(functionName, sqlTableFunctionResolver.getFunctionName());
        assertEquals(List.of(STRING), resolution.getKey().getParamTypeList());
        FunctionBuilder functionBuilder = resolution.getValue();
        TableFunctionImplementation functionImplementation
                = (TableFunctionImplementation) functionBuilder.apply(functionProperties, expressions);
        assertTrue(functionImplementation instanceof SqlFunctionImplementation);
        SparkMetricTable sparkMetricTable
                = (SparkMetricTable) functionImplementation.applyArguments();
        assertNotNull(sparkMetricTable.getSparkQueryRequest());
        SparkQueryRequest sparkQueryRequest =
                sparkMetricTable.getSparkQueryRequest();
        assertEquals("select 1", sparkQueryRequest.getSql());
    }

    @Test
    void testArgumentsPassedByPosition() {
        SqlTableFunctionResolver sqlTableFunctionResolver
                = new SqlTableFunctionResolver(client);
        FunctionName functionName = FunctionName.of("sql");
        List<Expression> expressions
                = List.of(DSL.namedArgument(null, DSL.literal("select 1")));
        FunctionSignature functionSignature = new FunctionSignature(functionName, expressions
                .stream().map(Expression::type).collect(Collectors.toList()));

        Pair<FunctionSignature, FunctionBuilder> resolution
                = sqlTableFunctionResolver.resolve(functionSignature);

        assertEquals(functionName, resolution.getKey().getFunctionName());
        assertEquals(functionName, sqlTableFunctionResolver.getFunctionName());
        assertEquals(List.of(STRING), resolution.getKey().getParamTypeList());
        FunctionBuilder functionBuilder = resolution.getValue();
        TableFunctionImplementation functionImplementation
                = (TableFunctionImplementation) functionBuilder.apply(functionProperties, expressions);
        assertTrue(functionImplementation instanceof SqlFunctionImplementation);
        SparkMetricTable sparkMetricTable
                = (SparkMetricTable) functionImplementation.applyArguments();
        assertNotNull(sparkMetricTable.getSparkQueryRequest());
        SparkQueryRequest sparkQueryRequest =
                sparkMetricTable.getSparkQueryRequest();
        assertEquals("select 1", sparkQueryRequest.getSql());
    }

    @Test
    void testMixedArgumentTypes() {
        SqlTableFunctionResolver sqlTableFunctionResolver
                = new SqlTableFunctionResolver(client);
        FunctionName functionName = FunctionName.of("sql");
        List<Expression> expressions
                = List.of(DSL.namedArgument("query", DSL.literal("select 1")),
                DSL.namedArgument(null, DSL.literal(12345)));
        FunctionSignature functionSignature = new FunctionSignature(functionName, expressions
                .stream().map(Expression::type).collect(Collectors.toList()));
        Pair<FunctionSignature, FunctionBuilder> resolution
                = sqlTableFunctionResolver.resolve(functionSignature);

        assertEquals(functionName, resolution.getKey().getFunctionName());
        assertEquals(functionName, sqlTableFunctionResolver.getFunctionName());
        assertEquals(List.of(STRING), resolution.getKey().getParamTypeList());
        SemanticCheckException exception = assertThrows(SemanticCheckException.class,
                () -> resolution.getValue().apply(functionProperties, expressions));

        assertEquals("Arguments should be either passed by name or position", exception.getMessage());
    }

    @Test
    void testWrongArgumentsSizeWhenPassedByName() {
        SqlTableFunctionResolver sqlTableFunctionResolver
                = new SqlTableFunctionResolver(client);
        FunctionName functionName = FunctionName.of("sql");
        List<Expression> expressions
                = List.of();
        FunctionSignature functionSignature = new FunctionSignature(functionName, expressions
                .stream().map(Expression::type).collect(Collectors.toList()));
        Pair<FunctionSignature, FunctionBuilder> resolution
                = sqlTableFunctionResolver.resolve(functionSignature);

        assertEquals(functionName, resolution.getKey().getFunctionName());
        assertEquals(functionName, sqlTableFunctionResolver.getFunctionName());
        assertEquals(List.of(STRING), resolution.getKey().getParamTypeList());
        SemanticCheckException exception = assertThrows(SemanticCheckException.class,
                () -> resolution.getValue().apply(functionProperties, expressions));

        assertEquals("Missing arguments:[query]", exception.getMessage());
    }

}
