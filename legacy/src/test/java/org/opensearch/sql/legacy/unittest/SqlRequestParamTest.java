/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.request.SqlRequestParam.QUERY_PARAMS_FORMAT;
import static org.opensearch.sql.legacy.request.SqlRequestParam.QUERY_PARAMS_PRETTY;

import com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.legacy.executor.Format;
import org.opensearch.sql.legacy.request.SqlRequestParam;

public class SqlRequestParamTest {
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void shouldReturnTrueIfPrettyParamsIsTrue() {
        assertTrue(SqlRequestParam.isPrettyFormat(ImmutableMap.of(QUERY_PARAMS_PRETTY, "true")));
    }

    @Test
    public void shouldReturnTrueIfPrettyParamsIsEmpty() {
        assertTrue(SqlRequestParam.isPrettyFormat(ImmutableMap.of(QUERY_PARAMS_PRETTY, "")));
    }

    @Test
    public void shouldReturnFalseIfNoPrettyParams() {
        assertFalse(SqlRequestParam.isPrettyFormat(ImmutableMap.of()));
    }

    @Test
    public void shouldReturnFalseIfPrettyParamsIsUnknownValue() {
        assertFalse(SqlRequestParam.isPrettyFormat(ImmutableMap.of(QUERY_PARAMS_PRETTY, "unknown")));
    }

    @Test
    public void shouldReturnJSONIfFormatParamsIsJSON() {
        assertEquals(Format.JSON, SqlRequestParam.getFormat(ImmutableMap.of(QUERY_PARAMS_FORMAT, "json")));
    }

    @Test
    public void shouldReturnDefaultFormatIfNoFormatParams() {
        assertEquals(Format.JDBC, SqlRequestParam.getFormat(ImmutableMap.of()));
    }

    @Test
    public void shouldThrowExceptionIfFormatParamsIsEmpty() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Failed to create executor due to unknown response format: ");

        assertEquals(Format.JDBC, SqlRequestParam.getFormat(ImmutableMap.of(QUERY_PARAMS_FORMAT, "")));
    }

    @Test
    public void shouldThrowExceptionIfFormatParamsIsNotSupported() {
        exceptionRule.expect(IllegalArgumentException.class);
        exceptionRule.expectMessage("Failed to create executor due to unknown response format: xml");

        SqlRequestParam.getFormat(ImmutableMap.of(QUERY_PARAMS_FORMAT, "xml"));
    }
}
