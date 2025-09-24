/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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

  // ============================================================================
  // NEW TESTS FOR JOIN_TIME_OUT HINT FUNCTIONALITY
  // ============================================================================

  @SneakyThrows
  @Test
  public void testCreateWithCustomTimeoutFromJoinHint() {
    // Test custom timeout from JOIN_TIME_OUT hint
    TimeValue customTimeout = TimeValue.timeValueSeconds(120);
    PointInTimeHandlerImpl customHandler =
        new PointInTimeHandlerImpl(mockClient, indices, customTimeout);

    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);

    customHandler.create();

    // Verify that the CreatePitRequest was called with custom timeout
    ArgumentCaptor<CreatePitRequest> requestCaptor =
        ArgumentCaptor.forClass(CreatePitRequest.class);
    verify(mockClient).execute(eq(CreatePitAction.INSTANCE), requestCaptor.capture());

    CreatePitRequest capturedRequest = requestCaptor.getValue();
    assertEquals("Custom timeout should be used", customTimeout, capturedRequest.getKeepAlive());
    assertEquals("PIT ID should be set", PIT_ID, customHandler.getPitId());
  }

  @SneakyThrows
  @Test
  public void testCreateWithDifferentJoinTimeoutValues() {
    // Test various JOIN_TIME_OUT hint values
    TimeValue[] testTimeouts = {
      TimeValue.timeValueSeconds(30), // 30 seconds
      TimeValue.timeValueSeconds(60), // 1 minute
      TimeValue.timeValueSeconds(120), // 2 minutes
      TimeValue.timeValueSeconds(300), // 5 minutes
      TimeValue.timeValueSeconds(600), // 10 minutes
      TimeValue.timeValueSeconds(1800) // 30 minutes
    };

    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);

    // Create all handlers and call create() on each
    for (TimeValue timeout : testTimeouts) {
      PointInTimeHandlerImpl customHandler =
          new PointInTimeHandlerImpl(mockClient, indices, timeout);
      customHandler.create();
    }

    // Verify all timeouts were used correctly
    ArgumentCaptor<CreatePitRequest> requestCaptor =
        ArgumentCaptor.forClass(CreatePitRequest.class);
    verify(mockClient, times(testTimeouts.length))
        .execute(eq(CreatePitAction.INSTANCE), requestCaptor.capture());

    List<CreatePitRequest> capturedRequests = requestCaptor.getAllValues();
    assertEquals(
        "Should have captured " + testTimeouts.length + " requests",
        testTimeouts.length,
        capturedRequests.size());

    // Verify each timeout was preserved
    for (int i = 0; i < testTimeouts.length; i++) {
      CreatePitRequest request = capturedRequests.get(i);
      TimeValue expectedTimeout = testTimeouts[i];
      assertEquals(
          "Timeout " + expectedTimeout + " should be preserved",
          expectedTimeout,
          request.getKeepAlive());
    }
  }

  @SneakyThrows
  @Test
  public void testCreateWithNullCustomTimeout() {
    // Test that null custom timeout falls back to default behavior
    PointInTimeHandlerImpl customHandler = new PointInTimeHandlerImpl(mockClient, indices, null);

    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);

    customHandler.create();

    // Should still work (using default from settings)
    verify(mockClient).execute(eq(CreatePitAction.INSTANCE), any(CreatePitRequest.class));
    assertEquals("PIT ID should be set even with null timeout", PIT_ID, customHandler.getPitId());
  }

  @SneakyThrows
  @Test
  public void testCreateDefaultVsCustomTimeout() {
    // Test default handler
    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);
    pointInTimeHandlerImpl.create();

    // Capture all calls and get the first one (default handler)
    ArgumentCaptor<CreatePitRequest> allRequestsCaptor =
        ArgumentCaptor.forClass(CreatePitRequest.class);

    // Test custom handler
    TimeValue customTimeout = TimeValue.timeValueSeconds(300);
    PointInTimeHandlerImpl customHandler =
        new PointInTimeHandlerImpl(mockClient, indices, customTimeout);

    customHandler.create();

    // Now verify that execute was called exactly 2 times and capture all requests
    verify(mockClient, times(2)).execute(eq(CreatePitAction.INSTANCE), allRequestsCaptor.capture());

    // Get both captured requests
    List<CreatePitRequest> capturedRequests = allRequestsCaptor.getAllValues();
    assertEquals("Should have captured 2 requests", 2, capturedRequests.size());

    CreatePitRequest defaultRequest = capturedRequests.get(0); // First call (default handler)
    CreatePitRequest customRequest = capturedRequests.get(1); // Second call (custom handler)

    // Verify the custom timeout is preserved
    assertEquals("Custom timeout should be preserved", customTimeout, customRequest.getKeepAlive());

    // Verify they are different (if possible)
    assertNotEquals(
        "Default and custom timeouts should be different",
        defaultRequest.getKeepAlive(),
        customRequest.getKeepAlive());
  }

  @Test
  public void testConstructorWithCustomTimeout() {
    TimeValue customTimeout = TimeValue.timeValueSeconds(180);
    PointInTimeHandlerImpl customHandler =
        new PointInTimeHandlerImpl(mockClient, indices, customTimeout);

    assertNotNull("Handler should not be null", customHandler);
    assertNull("PIT ID should be null initially", customHandler.getPitId());
    // Note: We can't directly access the custom timeout, but it will be used in create()
  }

  @Test
  public void testConstructorWithPitIdOnly() {
    String existingPitId = "existing_pit_id_12345";
    PointInTimeHandlerImpl pitHandler = new PointInTimeHandlerImpl(mockClient, existingPitId);

    assertNotNull("Handler should not be null", pitHandler);
    assertEquals("PIT ID should be set", existingPitId, pitHandler.getPitId());
  }

  @SneakyThrows
  @Test
  public void testTimeValueConversions() {
    // Test different TimeValue creation methods work correctly
    TimeValue secondsTimeout = TimeValue.timeValueSeconds(120); // 2 minutes
    TimeValue minutesTimeout = TimeValue.timeValueMinutes(3); // 3 minutes = 180 seconds
    TimeValue millisTimeout = TimeValue.timeValueMillis(150000); // 150 seconds

    TimeValue[] timeouts = {secondsTimeout, minutesTimeout, millisTimeout};

    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);

    // Create all handlers and call create() on each
    for (TimeValue timeout : timeouts) {
      PointInTimeHandlerImpl customHandler =
          new PointInTimeHandlerImpl(mockClient, indices, timeout);
      customHandler.create();
    }

    // Verify all TimeValue conversions work correctly
    ArgumentCaptor<CreatePitRequest> requestCaptor =
        ArgumentCaptor.forClass(CreatePitRequest.class);
    verify(mockClient, times(timeouts.length))
        .execute(eq(CreatePitAction.INSTANCE), requestCaptor.capture());

    List<CreatePitRequest> capturedRequests = requestCaptor.getAllValues();
    assertEquals(
        "Should have captured " + timeouts.length + " requests",
        timeouts.length,
        capturedRequests.size());

    // Verify each TimeValue was preserved regardless of creation method
    for (int i = 0; i < timeouts.length; i++) {
      CreatePitRequest request = capturedRequests.get(i);
      TimeValue expectedTimeout = timeouts[i];
      assertEquals(
          "TimeValue " + expectedTimeout + " should be preserved regardless of creation method",
          expectedTimeout,
          request.getKeepAlive());
    }
  }

  @SneakyThrows
  @Test
  public void testCompleteLifecycleWithCustomTimeout() {
    TimeValue customTimeout = TimeValue.timeValueSeconds(240);
    PointInTimeHandlerImpl customHandler =
        new PointInTimeHandlerImpl(mockClient, indices, customTimeout);

    // Initially no PIT ID
    assertNull("PIT ID should be null initially", customHandler.getPitId());

    // Create PIT
    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);
    customHandler.create();
    assertEquals("PIT ID should be set after creation", PIT_ID, customHandler.getPitId());

    // Delete PIT
    when(mockActionFutureDelete.get()).thenReturn(mockDeletePitResponse);
    customHandler.delete();

    // Verify both operations
    verify(mockClient).execute(eq(CreatePitAction.INSTANCE), any(CreatePitRequest.class));
    verify(mockClient).execute(eq(DeletePitAction.INSTANCE), any(DeletePitRequest.class));

    // Verify custom timeout was used
    ArgumentCaptor<CreatePitRequest> requestCaptor =
        ArgumentCaptor.forClass(CreatePitRequest.class);
    verify(mockClient).execute(eq(CreatePitAction.INSTANCE), requestCaptor.capture());
    assertEquals(
        "Custom timeout should be used", customTimeout, requestCaptor.getValue().getKeepAlive());
  }

  @SneakyThrows
  @Test
  public void testMultipleIndicesWithCustomTimeout() {
    String[] multipleIndices = {"index1", "index2", "index3", "test_left", "test_right"};
    TimeValue customTimeout = TimeValue.timeValueSeconds(360);

    PointInTimeHandlerImpl customHandler =
        new PointInTimeHandlerImpl(mockClient, multipleIndices, customTimeout);

    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);

    customHandler.create();

    ArgumentCaptor<CreatePitRequest> requestCaptor =
        ArgumentCaptor.forClass(CreatePitRequest.class);
    verify(mockClient).execute(eq(CreatePitAction.INSTANCE), requestCaptor.capture());

    CreatePitRequest request = requestCaptor.getValue();
    assertEquals(
        "Custom timeout should be used with multiple indices",
        customTimeout,
        request.getKeepAlive());
    assertEquals("PIT ID should be set", PIT_ID, customHandler.getPitId());
  }

  @SneakyThrows
  @Test
  public void testErrorHandlingWithCustomTimeout() {
    TimeValue customTimeout = TimeValue.timeValueSeconds(150);
    PointInTimeHandlerImpl customHandler =
        new PointInTimeHandlerImpl(mockClient, indices, customTimeout);

    // Setup failure scenario
    ExecutionException executionException =
        new ExecutionException("PIT creation failed with custom timeout", new RuntimeException());
    when(mockActionFuture.get()).thenThrow(executionException);

    RuntimeException thrownException =
        assertThrows(RuntimeException.class, () -> customHandler.create());

    verify(mockClient).execute(eq(CreatePitAction.INSTANCE), any(CreatePitRequest.class));
    assertNotNull("Exception should have a cause", thrownException.getCause());
    assertEquals(
        "Error message should be preserved",
        "Error occurred while creating PIT.",
        thrownException.getMessage());
  }

  @SneakyThrows
  @Test
  public void testJoinTimeoutHintScenario() {
    // Simulate the scenario where JOIN_TIME_OUT(120) hint is used
    // This creates a PIT with 120 seconds keepalive instead of default 60 seconds

    TimeValue joinTimeoutHint = TimeValue.timeValueSeconds(120);
    PointInTimeHandlerImpl leftTableHandler =
        new PointInTimeHandlerImpl(mockClient, new String[] {"test_left"}, joinTimeoutHint);
    PointInTimeHandlerImpl rightTableHandler =
        new PointInTimeHandlerImpl(mockClient, new String[] {"test_right"}, joinTimeoutHint);

    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);

    // Create PITs for both tables
    leftTableHandler.create();
    rightTableHandler.create();

    // Verify both handlers use the custom timeout
    ArgumentCaptor<CreatePitRequest> requestCaptor =
        ArgumentCaptor.forClass(CreatePitRequest.class);
    verify(mockClient, times(2)).execute(eq(CreatePitAction.INSTANCE), requestCaptor.capture());

    // Get both captured requests
    List<CreatePitRequest> capturedRequests = requestCaptor.getAllValues();
    assertEquals("Should have captured 2 requests", 2, capturedRequests.size());

    CreatePitRequest leftTableRequest = capturedRequests.get(0); // First call (left table)
    CreatePitRequest rightTableRequest = capturedRequests.get(1); // Second call (right table)

    // Verify both tables use the JOIN_TIME_OUT hint value
    assertEquals(
        "Left table should use JOIN_TIME_OUT hint value",
        joinTimeoutHint,
        leftTableRequest.getKeepAlive());
    assertEquals(
        "Right table should use JOIN_TIME_OUT hint value",
        joinTimeoutHint,
        rightTableRequest.getKeepAlive());
    assertEquals("Left table PIT should be created", PIT_ID, leftTableHandler.getPitId());
    assertEquals("Right table PIT should be created", PIT_ID, rightTableHandler.getPitId());
  }

  @SneakyThrows
  @Test
  public void testDefaultTimeoutWhenNoHint() {
    // Test the original behavior when no JOIN_TIME_OUT hint is provided
    PointInTimeHandlerImpl defaultHandler = new PointInTimeHandlerImpl(mockClient, indices);

    when(mockActionFuture.get()).thenReturn(mockCreatePitResponse);

    defaultHandler.create();

    // Should use default timeout from cluster settings
    verify(mockClient).execute(eq(CreatePitAction.INSTANCE), any(CreatePitRequest.class));
    assertEquals("PIT should be created with default timeout", PIT_ID, defaultHandler.getPitId());
  }
}
