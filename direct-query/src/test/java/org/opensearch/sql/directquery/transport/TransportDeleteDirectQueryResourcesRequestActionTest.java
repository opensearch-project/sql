/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.directquery.DirectQueryExecutorServiceImpl;
import org.opensearch.sql.directquery.rest.model.DeleteDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.DeleteDirectQueryResourcesResponse;
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.transport.model.DeleteDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.DeleteDirectQueryResourcesActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportDeleteDirectQueryResourcesRequestActionTest {

  @Mock private TransportService transportService;

  @Mock private ActionFilters actionFilters;

  @Mock private DirectQueryExecutorServiceImpl directQueryExecutorService;

  @Mock private Task task;

  private TransportDeleteDirectQueryResourcesRequestAction action;

  @BeforeEach
  public void setUp() {
    action =
        new TransportDeleteDirectQueryResourcesRequestAction(
            transportService, actionFilters, directQueryExecutorService);
  }

  @Test
  public void testDoExecuteSuccessful() throws Exception {
    DeleteDirectQueryResourcesRequest directQueryRequest =
        new DeleteDirectQueryResourcesRequest();
    directQueryRequest.setDataSource("testDataSource");
    directQueryRequest.setResourceType(DirectQueryResourceType.RULES);
    directQueryRequest.setNamespace("test_namespace");

    DeleteDirectQueryResourcesActionRequest actionRequest =
        new DeleteDirectQueryResourcesActionRequest(directQueryRequest);

    DeleteDirectQueryResourcesResponse serviceResponse =
        DeleteDirectQueryResourcesResponse.withMessage("{\"status\":\"success\"}");

    when(directQueryExecutorService.deleteDirectQueryResources(directQueryRequest))
        .thenReturn(serviceResponse);

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<DeleteDirectQueryResourcesActionResponse> responseRef =
        new AtomicReference<>();
    AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    ActionListener<DeleteDirectQueryResourcesActionResponse> listener =
        new ActionListener<DeleteDirectQueryResourcesActionResponse>() {
          @Override
          public void onResponse(DeleteDirectQueryResourcesActionResponse response) {
            responseRef.set(response);
            latch.countDown();
          }

          @Override
          public void onFailure(Exception e) {
            exceptionRef.set(e);
            latch.countDown();
          }
        };

    action.doExecute(task, actionRequest, listener);

    latch.await(5, TimeUnit.SECONDS);

    assertNotNull(responseRef.get());
    assertEquals(0, latch.getCount());
    verify(directQueryExecutorService).deleteDirectQueryResources(directQueryRequest);
  }

  @Test
  public void testDoExecuteWithException() throws Exception {
    DeleteDirectQueryResourcesRequest directQueryRequest =
        new DeleteDirectQueryResourcesRequest();
    directQueryRequest.setDataSource("testDataSource");
    directQueryRequest.setResourceType(DirectQueryResourceType.RULES);
    directQueryRequest.setNamespace("test_namespace");

    DeleteDirectQueryResourcesActionRequest actionRequest =
        new DeleteDirectQueryResourcesActionRequest(directQueryRequest);

    RuntimeException testException = new RuntimeException("Test exception");
    when(directQueryExecutorService.deleteDirectQueryResources(directQueryRequest))
        .thenThrow(testException);

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<DeleteDirectQueryResourcesActionResponse> responseRef =
        new AtomicReference<>();
    AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    ActionListener<DeleteDirectQueryResourcesActionResponse> listener =
        new ActionListener<DeleteDirectQueryResourcesActionResponse>() {
          @Override
          public void onResponse(DeleteDirectQueryResourcesActionResponse response) {
            responseRef.set(response);
            latch.countDown();
          }

          @Override
          public void onFailure(Exception e) {
            exceptionRef.set(e);
            latch.countDown();
          }
        };

    action.doExecute(task, actionRequest, listener);

    latch.await(5, TimeUnit.SECONDS);

    assertNotNull(exceptionRef.get());
    assertEquals(testException, exceptionRef.get());
    assertEquals(0, latch.getCount());
    verify(directQueryExecutorService).deleteDirectQueryResources(directQueryRequest);
  }
}
