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

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DOUBLE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.INTEGER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.KEYWORD;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.LONG;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.NUMBER;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TEXT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TYPE_ERROR;
import static org.opensearch.sql.legacy.antlr.semantic.types.function.ScalarFunction.LOG;

import org.junit.Test;

/**
 * Generic type test
 */
public class GenericTypeTest {

    @Test
    public void passNumberArgToLogShouldReturnNumber() {
        assertEquals(DOUBLE, LOG.construct(singletonList(NUMBER)));
    }

    @Test
    public void passIntegerArgToLogShouldReturnDouble() {
        assertEquals(DOUBLE, LOG.construct(singletonList(INTEGER)));
    }

    @Test
    public void passLongArgToLogShouldReturnDouble() {
        assertEquals(DOUBLE, LOG.construct(singletonList(LONG)));
    }

    @Test
    public void passTextArgToLogShouldReturnTypeError() {
        assertEquals(TYPE_ERROR, LOG.construct(singletonList(TEXT)));
    }

    @Test
    public void passKeywordArgToLogShouldReturnTypeError() {
        assertEquals(TYPE_ERROR, LOG.construct(singletonList(KEYWORD)));
    }

}
