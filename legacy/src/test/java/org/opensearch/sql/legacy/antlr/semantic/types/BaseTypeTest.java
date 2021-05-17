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

package org.opensearch.sql.legacy.antlr.semantic.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DATE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DOUBLE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.FLOAT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.KEYWORD;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.LONG;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NESTED;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.OPENSEARCH_TYPE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.SHORT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.STRING;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TEXT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.UNKNOWN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex.IndexType.NESTED_FIELD;

import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex;

/**
 * Test base type compatibility
 */
public class BaseTypeTest {

    @Test
    public void unknownTypeNameShouldReturnUnknown() {
        assertEquals(UNKNOWN, OpenSearchDataType.typeOf("this_is_a_new_es_type_we_arent_aware"));
    }

    @Test
    public void typeOfShouldIgnoreCase() {
        assertEquals(INTEGER, OpenSearchDataType.typeOf("Integer"));
    }

    @Test
    public void sameBaseTypeShouldBeCompatible() {
        assertTrue(INTEGER.isCompatible(INTEGER));
        assertTrue(BOOLEAN.isCompatible(BOOLEAN));
    }

    @Test
    public void parentBaseTypeShouldBeCompatibleWithSubBaseType() {
        assertTrue(NUMBER.isCompatible(DOUBLE));
        assertTrue(DOUBLE.isCompatible(FLOAT));
        assertTrue(FLOAT.isCompatible(INTEGER));
        assertTrue(INTEGER.isCompatible(SHORT));
        assertTrue(INTEGER.isCompatible(LONG));
        assertTrue(STRING.isCompatible(TEXT));
        assertTrue(STRING.isCompatible(KEYWORD));
        assertTrue(DATE.isCompatible(STRING));
    }

    @Test
    public void ancestorBaseTypeShouldBeCompatibleWithSubBaseType() {
        assertTrue(NUMBER.isCompatible(LONG));
        assertTrue(NUMBER.isCompatible(DOUBLE));
        assertTrue(DOUBLE.isCompatible(INTEGER));
        assertTrue(INTEGER.isCompatible(SHORT));
        assertTrue(INTEGER.isCompatible(LONG));
    }

    @Ignore("Two way compatibility is not necessary")
    @Test
    public void subBaseTypeShouldBeCompatibleWithParentBaseType() {
        assertTrue(KEYWORD.isCompatible(STRING));
    }

    @Test
    public void nonRelatedBaseTypeShouldNotBeCompatible() {
        assertFalse(SHORT.isCompatible(TEXT));
        assertFalse(DATE.isCompatible(BOOLEAN));
    }

    @Test
    public void unknownBaseTypeShouldBeCompatibleWithAnyBaseType() {
        assertTrue(UNKNOWN.isCompatible(INTEGER));
        assertTrue(UNKNOWN.isCompatible(KEYWORD));
        assertTrue(UNKNOWN.isCompatible(BOOLEAN));
    }

    @Test
    public void anyBaseTypeShouldBeCompatibleWithUnknownBaseType() {
        assertTrue(LONG.isCompatible(UNKNOWN));
        assertTrue(TEXT.isCompatible(UNKNOWN));
        assertTrue(DATE.isCompatible(UNKNOWN));
    }

    @Test
    public void nestedIndexTypeShouldBeCompatibleWithNestedDataType() {
        assertTrue(NESTED.isCompatible(new OpenSearchIndex("test", NESTED_FIELD)));
        assertTrue(OPENSEARCH_TYPE.isCompatible(new OpenSearchIndex("test", NESTED_FIELD)));
    }

}
