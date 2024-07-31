/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;

public class PointInTimeHandlerImplTest {

  @Mock private Client mockClient;
  private String[] indices = {"index1", "index2"};
  private PointInTimeHandlerImpl pointInTimeHandlerImpl;
  @Captor private ArgumentCaptor<ActionListener<DeletePitResponse>> listenerCaptor;
  private final String PIT_ID = "testId";

  /*@Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    pointInTimeHandlerImpl = new PointInTimeHandlerImpl(mockClient, indices);
  }

  @Test
  public void testCreate() {
    when(LocalClusterState.state().getSettingValue(SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(new TimeValue(10000));

    CreatePitResponse mockCreatePitResponse = mock(CreatePitResponse.class);
    when(mockCreatePitResponse.getId()).thenReturn(PIT_ID);

    CompletableFuture<CreatePitResponse> completableFuture =
        CompletableFuture.completedFuture(mockCreatePitResponse);

    doAnswer(
            invocation -> {
              ActionListener<CreatePitResponse> actionListener = invocation.getArgument(1);
              actionListener.onResponse(mockCreatePitResponse);
              return completableFuture;
            })
        .when(mockClient)
        .createPit(any(), any());

    pointInTimeHandlerImpl.create();

    assertEquals(PIT_ID, pointInTimeHandlerImpl.getPitId());
  }

  @Test
  public void testDelete() {
    DeletePitResponse mockedResponse = mock(DeletePitResponse.class);
    RestStatus mockRestStatus = mock(RestStatus.class);
    when(mockedResponse.status()).thenReturn(mockRestStatus);
    when(mockedResponse.status().getStatus()).thenReturn(200);
    pointInTimeHandlerImpl.setPitId(PIT_ID);
    pointInTimeHandlerImpl.delete();
    verify(mockClient).deletePits(any(), listenerCaptor.capture());
    listenerCaptor.getValue().onResponse(mockedResponse);
  }*/
}
