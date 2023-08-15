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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.model.transport.UpdateDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.UpdateDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportUpdateDataSourceActionTest {

  @Mock
  private TransportService transportService;
  @Mock
  private TransportUpdateDataSourceAction action;
  @Mock
  private DataSourceServiceImpl dataSourceService;
  @Mock
  private Task task;
  @Mock
  private ActionListener<UpdateDataSourceActionResponse> actionListener;

  @Captor
  private ArgumentCaptor<UpdateDataSourceActionResponse>
      updateDataSourceActionResponseArgumentCaptor;

  @Captor
  private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action = new TransportUpdateDataSourceAction(transportService,
        new ActionFilters(new HashSet<>()), dataSourceService);
  }

  @Test
  public void testDoExecute() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("test_datasource");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    UpdateDataSourceActionRequest request = new UpdateDataSourceActionRequest(dataSourceMetadata);

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).updateDataSource(dataSourceMetadata);
    Mockito.verify(actionListener)
        .onResponse(updateDataSourceActionResponseArgumentCaptor.capture());
    UpdateDataSourceActionResponse updateDataSourceActionResponse
        = updateDataSourceActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals("Updated DataSource with name test_datasource",
        updateDataSourceActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("test_datasource");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    doThrow(new RuntimeException("Error")).when(dataSourceService)
        .updateDataSource(dataSourceMetadata);
    UpdateDataSourceActionRequest request = new UpdateDataSourceActionRequest(dataSourceMetadata);
    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).updateDataSource(dataSourceMetadata);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error",
        exception.getMessage());
  }
}
