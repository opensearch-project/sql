/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.legacy.pit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.client.Client;

public class PointInTimeHandlerImplTest {

  @Mock private Client mockClient;
  private final String[] indices = {"index1", "index2"};
  private PointInTimeHandlerImpl pointInTimeHandlerImpl;
  private final String PIT_ID = "testId";
  private CreatePitResponse mockCreatePitResponse;
  private DeletePitResponse mockDeletePitResponse;
  private ActionFuture<CreatePitResponse> mockActionFuture;
  private ActionFuture<DeletePitResponse> mockActionFutureDelete;

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
    mockActionFuture = mock(ActionFuture.class);
    mockActionFutureDelete = mock(ActionFuture.class);
    when(mockClient.execute(any(CreatePitAction.class), any(CreatePitRequest.class)))
        .thenReturn(mockActionFuture);
    when(mockClient.execute(any(DeletePitAction.class), any(DeletePitRequest.class)))
        .thenReturn(mockActionFutureDelete);
    RestStatus mockRestStatus = mock(RestStatus.class);
    when(mockDeletePitResponse.status()).thenReturn(mockRestStatus);
    when(mockDeletePitResponse.status().getStatus()).thenReturn(200);
    when(mockCreatePitResponse.getId()).thenReturn(PIT_ID);
  }

  @SneakyThrows
  @Test
  public void testCreate() {
    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);
    try {
      pointInTimeHandlerImpl.create();
    } catch (RuntimeException e) {
      fail("Expected no exception while creating PIT, but got: " + e.getMessage());
    }
    verify(mockClient).execute(any(CreatePitAction.class), any(CreatePitRequest.class));
    verify(mockActionFuture).get();
    verify(mockCreatePitResponse).getId();
  }

  @SneakyThrows
  @Test
  public void testCreateForFailure() {
    ExecutionException executionException =
        new ExecutionException("Error occurred while creating PIT.", new Throwable());
    when(mockActionFuture.get()).thenThrow(executionException);

    RuntimeException thrownException =
        assertThrows(RuntimeException.class, () -> pointInTimeHandlerImpl.create());

    verify(mockClient).execute(any(CreatePitAction.class), any(CreatePitRequest.class));
    assertNotNull(thrownException.getCause());
    assertEquals("Error occurred while creating PIT.", thrownException.getMessage());
    verify(mockActionFuture).get();
  }

  @SneakyThrows
  @Test
  public void testDelete() {
    when(mockActionFutureDelete.get()).thenReturn(mockDeletePitResponse);
    try {
      pointInTimeHandlerImpl.delete();
    } catch (RuntimeException e) {
      fail("Expected no exception while deleting PIT, but got: " + e.getMessage());
    }
    verify(mockClient).execute(any(DeletePitAction.class), any(DeletePitRequest.class));
    verify(mockActionFutureDelete).get();
  }

  @SneakyThrows
  @Test
  public void testDeleteForFailure() {
    ExecutionException executionException =
        new ExecutionException("Error occurred while deleting PIT.", new Throwable());
    when(mockActionFutureDelete.get()).thenThrow(executionException);

    RuntimeException thrownException =
        assertThrows(RuntimeException.class, () -> pointInTimeHandlerImpl.delete());

    verify(mockClient).execute(any(DeletePitAction.class), any(DeletePitRequest.class));
    assertNotNull(thrownException.getCause());
    assertEquals("Error occurred while deleting PIT.", thrownException.getMessage());
    verify(mockActionFutureDelete).get();
  }
}
