/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.common.setting.Settings.Key.SQL_CURSOR_KEEP_ALIVE;

import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;

public class PointInTimeHandlerTest {

  @Mock private Client mockClient;
  private String[] indices = {"index1", "index2"};
  private PointInTimeHandler pointInTimeHandler;
  @Captor private ArgumentCaptor<ActionListener<DeletePitResponse>> listenerCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    pointInTimeHandler = new PointInTimeHandler(mockClient, indices);
  }

  @Test
  public void testCreate() {
    mockStatic(LocalClusterState.class);
    LocalClusterState localClusterState = mock(LocalClusterState.class);
    when(LocalClusterState.state()).thenReturn(localClusterState);
    when(LocalClusterState.state().getSettingValue(SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(new TimeValue(10000));

    CreatePitResponse mockCreatePitResponse = mock(CreatePitResponse.class);
    when(mockCreatePitResponse.getId()).thenReturn("testId");

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

    pointInTimeHandler.create();

    assertEquals("testId", pointInTimeHandler.getPitId());
  }

  @Test
  public void testDelete() {
    DeletePitResponse mockedResponse = mock(DeletePitResponse.class);
    pointInTimeHandler.delete();
    verify(mockClient).deletePits(any(), listenerCaptor.capture());
    listenerCaptor.getValue().onResponse(mockedResponse);
  }
}
