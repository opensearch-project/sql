/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy.antlr.semantic.scope;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DATE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.KEYWORD;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TEXT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex.IndexType.NESTED_FIELD;

import java.util.Map;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.TypeExpression;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex;

/**
 * Test cases for symbol table
 */
public class SymbolTableTest {

    private final SymbolTable symbolTable = new SymbolTable();

    @Test
    public void defineFieldSymbolShouldBeAbleToResolve() {
        defineSymbolShouldBeAbleToResolve(new Symbol(Namespace.FIELD_NAME, "birthday"), DATE);
    }

    @Test
    public void defineFunctionSymbolShouldBeAbleToResolve() {
        String funcName = "LOG";
        Type expectedType = new TypeExpression() {
            @Override
            public String getName() {
                return "Temp type expression with [NUMBER] -> NUMBER specification";
            }

            @Override
            public TypeExpressionSpec[] specifications() {
                return new TypeExpressionSpec[] {
                    new TypeExpressionSpec().map(NUMBER).to(NUMBER)
                };
            }
        };
        Symbol symbol = new Symbol(Namespace.FUNCTION_NAME, funcName);
        defineSymbolShouldBeAbleToResolve(symbol, expectedType);
    }

    @Test
    public void defineFieldSymbolShouldBeAbleToResolveByPrefix() {
        symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.projects"), new OpenSearchIndex("s.projects", NESTED_FIELD));
        symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.projects.release"), DATE);
        symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.projects.active"), BOOLEAN);
        symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.address"), TEXT);
        symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.city"), KEYWORD);
        symbolTable.store(new Symbol(Namespace.FIELD_NAME, "s.manager.name"), TEXT);

        Map<String, Type> typeByName = symbolTable.lookupByPrefix(new Symbol(Namespace.FIELD_NAME, "s.projects"));
        assertThat(
            typeByName,
            allOf(
                aMapWithSize(3),
                hasEntry("s.projects", (Type) new OpenSearchIndex("s.projects", NESTED_FIELD)),
                hasEntry("s.projects.release", DATE),
                hasEntry("s.projects.active", BOOLEAN)
            )
        );
    }

    private void defineSymbolShouldBeAbleToResolve(Symbol symbol, Type expectedType) {
        symbolTable.store(symbol, expectedType);

        Optional<Type> actualType = symbolTable.lookup(symbol);
        Assert.assertTrue(actualType.isPresent());
        Assert.assertEquals(expectedType, actualType.get());
    }

}
