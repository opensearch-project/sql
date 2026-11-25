/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical.node.pointInTime;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.executor.join.MetaSearchResult;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.resource.ResourceManager;
import org.opensearch.transport.client.Client;

public class PointInTimeTest {

  private static final String PIT_ID = "test-pit-id";

  private Client client;
  private SearchRequestBuilder searchRequestBuilder;
  private PointInTime pointInTime;
  private ExecuteParams params;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    client = mock(Client.class);

    CreatePitResponse createPitResponse = mock(CreatePitResponse.class);
    when(createPitResponse.getId()).thenReturn(PIT_ID);
    ActionFuture<CreatePitResponse> createFuture = mock(ActionFuture.class);
    when(createFuture.get()).thenReturn(createPitResponse);
    when(client.execute(eq(CreatePitAction.INSTANCE), any(CreatePitRequest.class)))
        .thenReturn(createFuture);

    DeletePitResponse deletePitResponse = mock(DeletePitResponse.class);
    when(deletePitResponse.status()).thenReturn(RestStatus.OK);
    ActionFuture<DeletePitResponse> deleteFuture = mock(ActionFuture.class);
    when(deleteFuture.get()).thenReturn(deletePitResponse);
    when(client.execute(eq(DeletePitAction.INSTANCE), any(DeletePitRequest.class)))
        .thenReturn(deleteFuture);

    searchRequestBuilder = mock(SearchRequestBuilder.class, RETURNS_SELF);

    Select select = mock(Select.class);
    when(select.getIndexArr()).thenReturn(new String[] {"logs"});

    TableInJoinRequestBuilder request = new TableInJoinRequestBuilder();
    request.setRequestBuilder(searchRequestBuilder);
    request.setOriginalSelect(select);
    // A JOIN_TIME_OUT hint gives the PIT its keep-alive, so the test needs no cluster settings.
    request.setHintJoinTimeout(TimeValue.timeValueSeconds(60));

    ResourceManager resourceManager = mock(ResourceManager.class);
    when(resourceManager.isHealthy()).thenReturn(true);
    when(resourceManager.isTimeout()).thenReturn(false);
    when(resourceManager.getMetaResult()).thenReturn(new MetaSearchResult());

    params = new ExecuteParams();
    params.add(ExecuteParams.ExecuteParamType.CLIENT, client);
    params.add(ExecuteParams.ExecuteParamType.RESOURCE_MANAGER, resourceManager);
    params.add(ExecuteParams.ExecuteParamType.TIMEOUT, 30);

    pointInTime = new PointInTime(request, 100);
    pointInTime.open(params);
  }

  @Test
  public void closeDeletesPitWhenFirstSearchFails() {
    when(searchRequestBuilder.get())
        .thenThrow(new OpenSearchStatusException("search refused", RestStatus.FORBIDDEN));

    assertThrows(IllegalStateException.class, () -> pointInTime.hasNext());
    pointInTime.close();

    assertPitDeletedOnce();
  }

  @Test
  public void closeDeletesPitOnceAfterSuccessfulSearch() {
    SearchResponse searchResponse = mock(SearchResponse.class);
    when(searchResponse.getHits()).thenReturn(SearchHits.empty());
    when(searchRequestBuilder.get()).thenReturn(searchResponse);

    assertFalse(pointInTime.hasNext());
    pointInTime.close();
    pointInTime.close();

    assertPitDeletedOnce();
  }

  @Test
  public void closeWithoutPitDoesNotDelete() {
    pointInTime.close();

    verify(client, never()).execute(eq(DeletePitAction.INSTANCE), any(DeletePitRequest.class));
  }

  private void assertPitDeletedOnce() {
    ArgumentCaptor<DeletePitRequest> captor = ArgumentCaptor.forClass(DeletePitRequest.class);
    verify(client).execute(eq(DeletePitAction.INSTANCE), captor.capture());
    assertArrayEquals(new String[] {PIT_ID}, captor.getValue().getPitIds().toArray());
  }
}
