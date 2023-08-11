/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
import org.opensearch.common.action.ActionFuture;
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
