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
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.storage.script.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.script.FilterScript;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class ExpressionFilterScriptFactoryTest {

  @Mock
  private SearchLookup searchLookup;

  @Mock
  private LeafSearchLookup leafSearchLookup;

  @Mock
  private LeafReaderContext leafReaderContext;

  private final Expression expression = DSL.literal(true);

  private final Map<String, Object> params = Collections.emptyMap();

  private final FilterScript.Factory factory = new ExpressionFilterScriptFactory(expression);

  @Test
  void should_return_deterministic_result() {
    assertTrue(factory.isResultDeterministic());
  }

  @Test
  void can_initialize_expression_filter_script() throws IOException {
    when(searchLookup.getLeafSearchLookup(leafReaderContext)).thenReturn(leafSearchLookup);

    FilterScript.LeafFactory leafFactory = factory.newFactory(params, searchLookup);
    FilterScript actualFilterScript = leafFactory.newInstance(leafReaderContext);

    assertEquals(
        new ExpressionFilterScript(expression, searchLookup, leafReaderContext, params),
        actualFilterScript
    );
  }

}
