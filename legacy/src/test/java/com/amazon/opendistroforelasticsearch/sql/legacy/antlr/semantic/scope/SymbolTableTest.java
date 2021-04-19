/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.scope;

import com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.Type;
import com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.TypeExpression;
import com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DATE;
import static com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.KEYWORD;
import static com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TEXT;
import static com.amazon.opendistroforelasticsearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex.IndexType.NESTED_FIELD;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;

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
