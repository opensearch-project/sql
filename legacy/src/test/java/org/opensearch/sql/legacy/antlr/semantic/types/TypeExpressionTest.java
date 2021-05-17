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
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DATE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DOUBLE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.GEO_POINT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.STRING;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.UNKNOWN;
import static org.opensearch.sql.legacy.antlr.semantic.types.special.Generic.T;

import java.util.Arrays;
import org.junit.Test;

/**
 * Test cases for default implementation methods in interface TypeExpression
 */
public class TypeExpressionTest {

    private final TypeExpression test123 = new TypeExpression() {

        @Override
        public String getName() {
            return "TEST123";
        }

        @Override
        public TypeExpressionSpec[] specifications() {
            return new TypeExpressionSpec[] {
                new TypeExpressionSpec().map(T(NUMBER)).to(T),
                new TypeExpressionSpec().map(STRING, BOOLEAN).to(DATE)
            };
        }
    };

    @Test
    public void emptySpecificationShouldAlwaysReturnUnknown() {
        TypeExpression expr = new TypeExpression() {
            @Override
            public TypeExpressionSpec[] specifications() {
                return new TypeExpressionSpec[0];
            }

            @Override
            public String getName() {
                return "Temp type expression with empty specification";
            }
        };
        assertEquals(UNKNOWN, expr.construct(Arrays.asList(NUMBER)));
        assertEquals(UNKNOWN, expr.construct(Arrays.asList(STRING, BOOLEAN)));
        assertEquals(UNKNOWN, expr.construct(Arrays.asList(INTEGER, DOUBLE, GEO_POINT)));
    }

    @Test
    public void compatibilityCheckShouldPassIfAnySpecificationCompatible() {
        assertEquals(DOUBLE, test123.construct(Arrays.asList(DOUBLE)));
        assertEquals(DATE, test123.construct(Arrays.asList(STRING, BOOLEAN)));
    }

    @Test
    public void compatibilityCheckShouldFailIfNoSpecificationCompatible() {
        assertEquals(TYPE_ERROR, test123.construct(Arrays.asList(BOOLEAN)));
    }

    @Test
    public void usageShouldPrintAllSpecifications() {
        assertEquals("TEST123(NUMBER T) -> T or TEST123(STRING, BOOLEAN) -> DATE", test123.usage());
    }

}
