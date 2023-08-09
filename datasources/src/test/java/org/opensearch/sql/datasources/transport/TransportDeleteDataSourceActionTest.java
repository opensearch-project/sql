package org.opensearch.sql.datasources.transport;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.sql.datasources.model.transport.DeleteDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.DeleteDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportDeleteDataSourceActionTest {

  @Mock
  private TransportService transportService;
  @Mock
  private TransportDeleteDataSourceAction action;
  @Mock
  private DataSourceServiceImpl dataSourceService;
  @Mock
  private Task task;
  @Mock
  private ActionListener<DeleteDataSourceActionResponse> actionListener;

  @Captor
  private ArgumentCaptor<DeleteDataSourceActionResponse>
      deleteDataSourceActionResponseArgumentCaptor;
  @Captor
  private ArgumentCaptor<Exception> exceptionArgumentCaptor;


  @BeforeEach
  public void setUp() {
    action = new TransportDeleteDataSourceAction(transportService,
        new ActionFilters(new HashSet<>()), dataSourceService);
  }

  @Test
  public void testDoExecute() {
    DeleteDataSourceActionRequest request = new DeleteDataSourceActionRequest("test_datasource");

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).deleteDataSource("test_datasource");
    Mockito.verify(actionListener)
        .onResponse(deleteDataSourceActionResponseArgumentCaptor.capture());
    DeleteDataSourceActionResponse deleteDataSourceActionResponse
        = deleteDataSourceActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals("Deleted DataSource with name test_datasource",
        deleteDataSourceActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    doThrow(new RuntimeException("Error")).when(dataSourceService).deleteDataSource("testDS");
    DeleteDataSourceActionRequest request = new DeleteDataSourceActionRequest("testDS");
    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).deleteDataSource("testDS");
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error",
        exception.getMessage());
  }
}
