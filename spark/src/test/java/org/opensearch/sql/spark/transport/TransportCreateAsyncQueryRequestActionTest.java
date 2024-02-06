/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.MOCK_SESSION_ID;

import java.util.HashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorServiceImpl;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.transport.model.CreateAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CreateAsyncQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportCreateAsyncQueryRequestActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportCreateAsyncQueryRequestAction action;
  @Mock private AsyncQueryExecutorServiceImpl jobExecutorService;
  @Mock private Task task;
  @Mock private ActionListener<CreateAsyncQueryActionResponse> actionListener;
  @Mock private OpenSearchSettings pluginSettings;

  @Captor
  private ArgumentCaptor<CreateAsyncQueryActionResponse> createJobActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportCreateAsyncQueryRequestAction(
            transportService, new ActionFilters(new HashSet<>()), jobExecutorService, pluginSettings);
  }

  @Test
  public void testDoExecute() {
    CreateAsyncQueryRequest createAsyncQueryRequest =
        new CreateAsyncQueryRequest("source = my_glue.default.alb_logs", "my_glue", LangType.SQL);
    CreateAsyncQueryActionRequest request =
        new CreateAsyncQueryActionRequest(createAsyncQueryRequest);
    when(pluginSettings.getSettingValue(Settings.Key.ASYNC_QUERY_ENABLED)).thenReturn(true);
    when(jobExecutorService.createAsyncQuery(createAsyncQueryRequest))
        .thenReturn(new CreateAsyncQueryResponse("123", null));
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    CreateAsyncQueryActionResponse createAsyncQueryActionResponse =
        createJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "{\n" + "  \"queryId\": \"123\"\n" + "}", createAsyncQueryActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithSessionId() {
    CreateAsyncQueryRequest createAsyncQueryRequest =
        new CreateAsyncQueryRequest(
            "source = my_glue.default.alb_logs", "my_glue", LangType.SQL, MOCK_SESSION_ID);
    CreateAsyncQueryActionRequest request =
        new CreateAsyncQueryActionRequest(createAsyncQueryRequest);
    when(pluginSettings.getSettingValue(Settings.Key.ASYNC_QUERY_ENABLED)).thenReturn(true);
    when(jobExecutorService.createAsyncQuery(createAsyncQueryRequest))
        .thenReturn(new CreateAsyncQueryResponse("123", MOCK_SESSION_ID));
    action.doExecute(task, request, actionListener);
    Mockito.verify(actionListener).onResponse(createJobActionResponseArgumentCaptor.capture());
    CreateAsyncQueryActionResponse createAsyncQueryActionResponse =
        createJobActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "{\n" + "  \"queryId\": \"123\",\n" + "  \"sessionId\": \"s-0123456\"\n" + "}",
        createAsyncQueryActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    CreateAsyncQueryRequest createAsyncQueryRequest =
        new CreateAsyncQueryRequest("source = my_glue.default.alb_logs", "my_glue", LangType.SQL);
    CreateAsyncQueryActionRequest request =
        new CreateAsyncQueryActionRequest(createAsyncQueryRequest);
    when(pluginSettings.getSettingValue(Settings.Key.ASYNC_QUERY_ENABLED)).thenReturn(true);
    doThrow(new RuntimeException("Error"))
        .when(jobExecutorService)
        .createAsyncQuery(createAsyncQueryRequest);
    action.doExecute(task, request, actionListener);
    verify(jobExecutorService, times(1)).createAsyncQuery(createAsyncQueryRequest);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }

  @Test
  public void asyncQueryDisabled() {
    CreateAsyncQueryRequest createAsyncQueryRequest =
        new CreateAsyncQueryRequest("source = my_glue.default.alb_logs", "my_glue", LangType.SQL);
    CreateAsyncQueryActionRequest request =
        new CreateAsyncQueryActionRequest(createAsyncQueryRequest);
    when(pluginSettings.getSettingValue(Settings.Key.ASYNC_QUERY_ENABLED)).thenReturn(false);
    action.doExecute(task, request, actionListener);
    verify(jobExecutorService, never()).createAsyncQuery(createAsyncQueryRequest);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof IllegalAccessException);
    Assertions.assertEquals("plugins.query.executionengine.async_query.enabled " +
        "setting is false", exception.getMessage());
  }
}
