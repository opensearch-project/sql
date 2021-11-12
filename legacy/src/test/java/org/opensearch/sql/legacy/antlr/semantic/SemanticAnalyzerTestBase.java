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
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.antlr.semantic;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.util.CheckScriptContents.mockLocalClusterState;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Optional;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.legacy.antlr.OpenSearchLegacySqlAnalyzer;
import org.opensearch.sql.legacy.antlr.SqlAnalysisConfig;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;

/**
 * Test cases for semantic analysis focused on semantic check which was missing in the past.
 */
public abstract class SemanticAnalyzerTestBase {

    private static final String TEST_MAPPING_FILE = "mappings/semantics.json";

    /** public accessor is required by @Rule annotation */
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private OpenSearchLegacySqlAnalyzer
        analyzer = new OpenSearchLegacySqlAnalyzer(new SqlAnalysisConfig(true, true, 1000));

    @SuppressWarnings("UnstableApiUsage")
    @BeforeClass
    public static void init() throws IOException {
        URL url = Resources.getResource(TEST_MAPPING_FILE);
        String mappings = Resources.toString(url, Charsets.UTF_8);
        LocalClusterState.state(null);
        mockLocalClusterState(mappings);
    }

    @AfterClass
    public static void cleanUp() {
        LocalClusterState.state(null);
    }

    protected void expectValidationFailWithErrorMessages(String query, String... messages) {
        exception.expect(SemanticAnalysisException.class);
        exception.expectMessage(allOf(Arrays.stream(messages).
                                      map(Matchers::containsString).
                                      collect(toList())));
        validate(query);
    }

    protected void validate(String sql) {
        analyzer.analyze(sql, LocalClusterState.state());
    }

    protected void validateWithType(String sql, Type type) {
        Optional<Type> analyze = analyzer.analyze(sql, LocalClusterState.state());
        assertTrue(analyze.isPresent());
        assertEquals(type, analyze.get());
    }
}
