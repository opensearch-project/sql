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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.legacy.unittest.planner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.legacy.domain.ColumnTypeProvider;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.planner.core.BindingTupleQueryPlanner;
import org.opensearch.sql.legacy.util.AggregationUtils;
import org.opensearch.sql.legacy.util.SqlParserUtils;

@RunWith(MockitoJUnitRunner.class)
public class BindingTupleQueryPlannerExecuteTest {
    @Mock
    private Client client;
    @Mock
    private SearchResponse aggResponse;
    @Mock
    private ColumnTypeProvider columnTypeProvider;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        ActionFuture mockFuture = mock(ActionFuture.class);
        when(client.execute(any(), any())).thenReturn(mockFuture);
        when(mockFuture.actionGet()).thenAnswer(invocationOnMock -> aggResponse);
    }

    @Test
    public void testAggregationShouldPass() {
        assertThat(query("SELECT gender, MAX(age) as max, MIN(age) as min FROM accounts GROUP BY gender",
                         mockSearchAggregation()),
                   containsInAnyOrder(
                           BindingTuple.from(ImmutableMap.of("gender", "m", "max", 20d, "min", 10d)),
                           BindingTuple.from(ImmutableMap.of("gender", "f", "max", 40d, "min", 20d))));
    }


    protected List<BindingTuple> query(String sql, MockSearchAggregation mockAgg) {
        doAnswer(mockAgg).when(aggResponse).getAggregations();

        BindingTupleQueryPlanner queryPlanner =
                new BindingTupleQueryPlanner(client, SqlParserUtils.parse(sql), columnTypeProvider);
        return queryPlanner.execute();
    }

    private MockSearchAggregation mockSearchAggregation() {
        return new MockSearchAggregation("{\n"
                                         + "  \"sterms#gender\": {\n"
                                         + "    \"buckets\": [\n"
                                         + "      {\n"
                                         + "        \"key\": \"m\",\n"
                                         + "        \"doc_count\": 507,\n"
                                         + "        \"min#min\": {\n"
                                         + "          \"value\": 10\n"
                                         + "        },\n"
                                         + "        \"max#max\": {\n"
                                         + "          \"value\": 20\n"
                                         + "        }\n"
                                         + "      },\n"
                                         + "      {\n"
                                         + "        \"key\": \"f\",\n"
                                         + "        \"doc_count\": 493,\n"
                                         + "        \"min#min\": {\n"
                                         + "          \"value\": 20\n"
                                         + "        },\n"
                                         + "        \"max#max\": {\n"
                                         + "          \"value\": 40\n"
                                         + "        }\n"
                                         + "      }\n"
                                         + "    ]\n"
                                         + "  }\n"
                                         + "}");
    }

    protected static class MockSearchAggregation implements Answer<Aggregations> {
        private final Aggregations aggregation;

        public MockSearchAggregation(String agg) {
            aggregation = AggregationUtils.fromJson(agg);
        }

        @Override
        public Aggregations answer(InvocationOnMock invocationOnMock) throws Throwable {
            return aggregation;
        }
    }
}
