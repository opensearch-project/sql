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

package org.opensearch.sql.legacy.unittest;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.sql.legacy.esdomain.OpenSearchClient;

public class OpenSearchClientTest {

    @Mock
    protected Client client;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        ActionFuture<MultiSearchResponse> mockFuture = mock(ActionFuture.class);
        when(client.multiSearch(any())).thenReturn(mockFuture);

        MultiSearchResponse response = mock(MultiSearchResponse.class);
        when(mockFuture.actionGet()).thenReturn(response);

        MultiSearchResponse.Item item0 = new MultiSearchResponse.Item(mock(SearchResponse.class), null);
        MultiSearchResponse.Item item1 = new MultiSearchResponse.Item(mock(SearchResponse.class), new Exception());
        MultiSearchResponse.Item[] itemsRetry0 = new MultiSearchResponse.Item[]{item0, item1};
        MultiSearchResponse.Item[] itemsRetry1 = new MultiSearchResponse.Item[]{item0};
        when(response.getResponses()).thenAnswer(new Answer<MultiSearchResponse.Item[]>() {
            private int callCnt;

            @Override
            public MultiSearchResponse.Item[] answer(InvocationOnMock invocation) {
                return callCnt++ == 0 ? itemsRetry0 : itemsRetry1;
            }
        });
    }

    @Test
    public void multiSearchRetryOneTime() {
        OpenSearchClient openSearchClient = new OpenSearchClient(client);
        MultiSearchResponse.Item[] res = openSearchClient.multiSearch(new MultiSearchRequest().add(new SearchRequest()).add(new SearchRequest()));
        Assert.assertEquals(res.length, 2);
        Assert.assertFalse(res[0].isFailure());
        Assert.assertFalse(res[1].isFailure());
    }

}
