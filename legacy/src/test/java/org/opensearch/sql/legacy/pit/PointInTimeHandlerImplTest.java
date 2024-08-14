/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.legacy.pit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.util.Collections;
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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

public class PointInTimeHandlerImplTest {

  @Mock private Client mockClient;
  private String[] indices = {"index1", "index2"};
  private PointInTimeHandlerImpl pointInTimeHandlerImpl;
  @Captor private ArgumentCaptor<ActionListener<DeletePitResponse>> listenerCaptorForDelete;
  @Captor private ArgumentCaptor<ActionListener<CreatePitResponse>> listenerCaptorForCreate;
  private final String PIT_ID = "testId";
  private CreatePitResponse mockCreatePitResponse;
  private CompletableFuture<CreatePitResponse> completableFuture;
  private CompletableFuture<DeletePitResponse> completableFutureForDelete;
  private Exception exception;
  private DeletePitResponse mockDeletePitResponse;

  @Mock private OpenSearchSettings settings;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    pointInTimeHandlerImpl = new PointInTimeHandlerImpl(mockClient, indices);

    doReturn(Collections.emptyList()).when(settings).getSettings();
    when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(new TimeValue(10000));
    LocalClusterState.state().setPluginSettings(settings);

    mockCreatePitResponse = mock(CreatePitResponse.class);
    mockDeletePitResponse = mock(DeletePitResponse.class);
    RestStatus mockRestStatus = mock(RestStatus.class);
    when(mockDeletePitResponse.status()).thenReturn(mockRestStatus);
    when(mockDeletePitResponse.status().getStatus()).thenReturn(200);
    when(mockCreatePitResponse.getId()).thenReturn(PIT_ID);

    completableFuture = CompletableFuture.completedFuture(mockCreatePitResponse);
    completableFutureForDelete = CompletableFuture.completedFuture(mockDeletePitResponse);
    exception = mock(Exception.class);
  }

  @Test
  public void testCreate() {
    doAnswer(
            invocation -> {
              ActionListener<CreatePitResponse> actionListener = invocation.getArgument(1);
              actionListener.onResponse(mockCreatePitResponse);
              return completableFuture;
            })
        .when(mockClient)
        .createPit(any(), listenerCaptorForCreate.capture());

    boolean status = pointInTimeHandlerImpl.create();
    verify(mockClient).createPit(any(), listenerCaptorForCreate.capture());
    listenerCaptorForCreate.getValue().onResponse(mockCreatePitResponse);
    verify(mockCreatePitResponse, times(2)).getId();
    assertTrue(status);
  }

  @Test
  public void testCreateForFailure() {
    doAnswer(
            invocation -> {
              ActionListener<CreatePitResponse> actionListener = invocation.getArgument(1);
              actionListener.onFailure(exception);
              return completableFuture;
            })
        .when(mockClient)
        .createPit(any(), listenerCaptorForCreate.capture());

    boolean status = pointInTimeHandlerImpl.create();
    verify(mockClient).createPit(any(), listenerCaptorForCreate.capture());
    listenerCaptorForCreate.getValue().onResponse(mockCreatePitResponse);
    assertFalse(status);
  }

  @Test
  public void testDelete() {
    doAnswer(
            invocation -> {
              ActionListener<DeletePitResponse> actionListener = invocation.getArgument(1);
              actionListener.onResponse(mockDeletePitResponse);
              return completableFutureForDelete;
            })
        .when(mockClient)
        .deletePits(any(), listenerCaptorForDelete.capture());

    boolean status = pointInTimeHandlerImpl.delete();
    assertTrue(status);
    verify(mockClient).deletePits(any(), listenerCaptorForDelete.capture());
    listenerCaptorForDelete.getValue().onResponse(mockDeletePitResponse);
  }

  @Test
  public void testDeleteForFailure() {
    doAnswer(
            invocation -> {
              ActionListener<DeletePitResponse> actionListener = invocation.getArgument(1);
              actionListener.onFailure(exception);
              return completableFutureForDelete;
            })
        .when(mockClient)
        .deletePits(any(), listenerCaptorForDelete.capture());

    boolean status = pointInTimeHandlerImpl.delete();
    assertFalse(status);
    verify(mockClient).deletePits(any(), listenerCaptorForDelete.capture());
    listenerCaptorForDelete.getValue().onResponse(mockDeletePitResponse);
  }
}
