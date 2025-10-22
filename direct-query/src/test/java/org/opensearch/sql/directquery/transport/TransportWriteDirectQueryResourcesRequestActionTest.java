/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
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
import org.opensearch.sql.directquery.DirectQueryExecutorService;
import org.opensearch.sql.directquery.DirectQueryExecutorServiceImpl;
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.rest.model.WriteDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.WriteDirectQueryResourcesResponse;
import org.opensearch.sql.directquery.transport.model.WriteDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.WriteDirectQueryResourcesActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportWriteDirectQueryResourcesRequestActionTest {

  @Mock
  private TransportService transportService;

  @Mock
  private ActionFilters actionFilters;

  @Mock
  private DirectQueryExecutorServiceImpl directQueryExecutorService;

  @Mock
  private Task task;

  private TransportWriteDirectQueryResourcesRequestAction action;

  @BeforeEach
  public void setUp() {
    action = new TransportWriteDirectQueryResourcesRequestAction(
        transportService, actionFilters, directQueryExecutorService);
  }

  @Test
  public void testDoExecuteSuccessful() throws Exception {
    WriteDirectQueryResourcesRequest directQueryRequest = new WriteDirectQueryResourcesRequest();
    directQueryRequest.setDataSource("testDataSource");
    directQueryRequest.setResourceType(DirectQueryResourceType.ALERTMANAGER_SILENCES);
    directQueryRequest.setRequest("{\"matchers\":[{\"name\":\"alertname\",\"value\":\"TestAlert\"}],\"comment\":\"Test silence\"}");

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(directQueryRequest);

    WriteDirectQueryResourcesResponse serviceResponse =
        WriteDirectQueryResourcesResponse.withStringList(Arrays.asList("silence-12345"));

    when(directQueryExecutorService.writeDirectQueryResources(directQueryRequest))
        .thenReturn(serviceResponse);

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<WriteDirectQueryResourcesActionResponse> responseRef =
        new AtomicReference<>();
    AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    ActionListener<WriteDirectQueryResourcesActionResponse> listener =
        new ActionListener<WriteDirectQueryResourcesActionResponse>() {
          @Override
          public void onResponse(WriteDirectQueryResourcesActionResponse response) {
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
    verify(directQueryExecutorService).writeDirectQueryResources(directQueryRequest);
  }

  @Test
  public void testDoExecuteWithException() throws Exception {
    WriteDirectQueryResourcesRequest directQueryRequest = new WriteDirectQueryResourcesRequest();
    directQueryRequest.setDataSource("testDataSource");
    directQueryRequest.setResourceType(DirectQueryResourceType.ALERTMANAGER_SILENCES);

    WriteDirectQueryResourcesActionRequest actionRequest =
        new WriteDirectQueryResourcesActionRequest(directQueryRequest);

    RuntimeException testException = new RuntimeException("Test exception");
    when(directQueryExecutorService.writeDirectQueryResources(directQueryRequest))
        .thenThrow(testException);

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<WriteDirectQueryResourcesActionResponse> responseRef =
        new AtomicReference<>();
    AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    ActionListener<WriteDirectQueryResourcesActionResponse> listener =
        new ActionListener<WriteDirectQueryResourcesActionResponse>() {
          @Override
          public void onResponse(WriteDirectQueryResourcesActionResponse response) {
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
    verify(directQueryExecutorService).writeDirectQueryResources(directQueryRequest);
  }
}