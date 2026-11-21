/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.sql.legacy.esdomain.OpenSearchClient;
import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;
import org.opensearch.transport.client.Client;

public class OpenSearchClientTest {

  @Mock protected Client client;

  /** Skips the real back-off sleeps between retries, so retry tests don't wait seconds. */
  private MockedStatic<BackOffRetryStrategy> noBackOffSleep;

  @Before
  public void init() {
    noBackOffSleep = Mockito.mockStatic(BackOffRetryStrategy.class);
    MockitoAnnotations.initMocks(this);
    ActionFuture<MultiSearchResponse> mockFuture = mock(ActionFuture.class);
    when(client.multiSearch(any())).thenReturn(mockFuture);

    MultiSearchResponse response = mock(MultiSearchResponse.class);
    when(mockFuture.actionGet()).thenReturn(response);

    MultiSearchResponse.Item item0 = new MultiSearchResponse.Item(mock(SearchResponse.class), null);
    MultiSearchResponse.Item item1 =
        new MultiSearchResponse.Item(mock(SearchResponse.class), new Exception());
    MultiSearchResponse.Item[] itemsRetry0 = new MultiSearchResponse.Item[] {item0, item1};
    MultiSearchResponse.Item[] itemsRetry1 = new MultiSearchResponse.Item[] {item0};
    when(response.getResponses())
        .thenAnswer(
            new Answer<MultiSearchResponse.Item[]>() {
              private int callCnt;

              @Override
              public MultiSearchResponse.Item[] answer(InvocationOnMock invocation) {
                return callCnt++ == 0 ? itemsRetry0 : itemsRetry1;
              }
            });
  }

  @After
  public void closeStaticMocks() {
    noBackOffSleep.close();
  }

  @Test
  public void multiSearchRetryOneTime() {
    OpenSearchClient openSearchClient = new OpenSearchClient(client);
    MultiSearchResponse.Item[] res =
        openSearchClient.multiSearch(
            new MultiSearchRequest().add(new SearchRequest()).add(new SearchRequest()));
    Assert.assertEquals(res.length, 2);
    Assert.assertFalse(res[0].isFailure());
    Assert.assertFalse(res[1].isFailure());
  }

  @Test
  public void multiSearchDoesNotRetryClientErrorAndThrowsIt() {
    OpenSearchSecurityException denied =
        new OpenSearchSecurityException(
            "no permissions for [indices:data/read/search]", RestStatus.FORBIDDEN);
    Client failingClient = clientFailingEveryItemWith(denied);

    OpenSearchSecurityException thrown =
        assertThrows(
            OpenSearchSecurityException.class,
            () -> new OpenSearchClient(failingClient).multiSearch(oneRequest()));

    assertSame(denied, thrown);
    verify(failingClient, times(1)).multiSearch(any());
  }

  @Test
  public void multiSearchDoesNotRetryMissingIndexAndThrowsIt() {
    IndexNotFoundException missing = new IndexNotFoundException("logs");
    Client failingClient = clientFailingEveryItemWith(missing);

    IndexNotFoundException thrown =
        assertThrows(
            IndexNotFoundException.class,
            () -> new OpenSearchClient(failingClient).multiSearch(oneRequest()));

    assertSame(missing, thrown);
    verify(failingClient, times(1)).multiSearch(any());
  }

  @Test
  public void multiSearchRetriesTooManyRequests() {
    Client failingClient =
        clientFailingEveryItemWith(
            new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS));

    MultiSearchResponse.Item[] res = new OpenSearchClient(failingClient).multiSearch(oneRequest());

    assertEquals(1, res.length);
    assertTrue(res[0].isFailure());
    verify(failingClient, times(4)).multiSearch(any());
  }

  @Test
  public void multiSearchKeepsBadRequestOfOneItemAsItsFailedResponse() {
    // A value from one row of the first join table can make only that item fail with a 400.
    QueryShardException badValue =
        new QueryShardException(
            new Index("logs", "_na_"), "failed to create query: For input string: \"abc\"", null);
    ActionFuture<MultiSearchResponse> firstAttempt =
        futureOf(
            new MultiSearchResponse.Item(mock(SearchResponse.class), null),
            new MultiSearchResponse.Item(null, badValue),
            new MultiSearchResponse.Item(mock(SearchResponse.class), null));
    ActionFuture<MultiSearchResponse> retry =
        futureOf(new MultiSearchResponse.Item(null, badValue));
    Client mixedClient = mock(Client.class);
    when(mixedClient.multiSearch(any()))
        .thenAnswer(
            invocation ->
                invocation.<MultiSearchRequest>getArgument(0).requests().size() == 3
                    ? firstAttempt
                    : retry);

    MultiSearchResponse.Item[] res =
        new OpenSearchClient(mixedClient)
            .multiSearch(
                new MultiSearchRequest()
                    .add(new SearchRequest())
                    .add(new SearchRequest())
                    .add(new SearchRequest()));

    assertEquals(3, res.length);
    assertFalse(res[0].isFailure());
    assertTrue(res[1].isFailure());
    assertSame(badValue, res[1].getFailure());
    assertNull(res[1].getResponse());
    assertFalse(res[2].isFailure());
    verify(mixedClient, times(4)).multiSearch(any());
  }

  @Test
  public void multiSearchRetriesServerError() {
    Client failingClient =
        clientFailingEveryItemWith(
            new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE));

    MultiSearchResponse.Item[] res = new OpenSearchClient(failingClient).multiSearch(oneRequest());

    assertTrue(res[0].isFailure());
    verify(failingClient, times(4)).multiSearch(any());
  }

  private static MultiSearchRequest oneRequest() {
    return new MultiSearchRequest().add(new SearchRequest());
  }

  private static ActionFuture<MultiSearchResponse> futureOf(MultiSearchResponse.Item... items) {
    ActionFuture<MultiSearchResponse> future = mock(ActionFuture.class);
    MultiSearchResponse response = mock(MultiSearchResponse.class);
    when(future.actionGet()).thenReturn(response);
    when(response.getResponses()).thenReturn(items);
    return future;
  }

  private static Client clientFailingEveryItemWith(Exception failure) {
    ActionFuture<MultiSearchResponse> future =
        futureOf(new MultiSearchResponse.Item(null, failure));
    Client failingClient = mock(Client.class);
    when(failingClient.multiSearch(any())).thenReturn(future);
    return failingClient;
  }
}
