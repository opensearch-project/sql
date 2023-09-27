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
import org.opensearch.sql.datasources.model.transport.CreateDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.CreateDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportCreateDataSourceActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportCreateDataSourceAction action;
  @Mock private DataSourceServiceImpl dataSourceService;
  @Mock private Task task;
  @Mock private ActionListener<CreateDataSourceActionResponse> actionListener;

  @Captor
  private ArgumentCaptor<CreateDataSourceActionResponse>
      createDataSourceActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportCreateDataSourceAction(
            transportService, new ActionFilters(new HashSet<>()), dataSourceService);
  }

  @Test
  public void testDoExecute() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("test_datasource");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    CreateDataSourceActionRequest request = new CreateDataSourceActionRequest(dataSourceMetadata);

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).createDataSource(dataSourceMetadata);
    Mockito.verify(actionListener)
        .onResponse(createDataSourceActionResponseArgumentCaptor.capture());
    CreateDataSourceActionResponse createDataSourceActionResponse =
        createDataSourceActionResponseArgumentCaptor.getValue();
    JsonResponseFormatter<String> jsonResponseFormatter =
        new JsonResponseFormatter<>(JsonResponseFormatter.Style.PRETTY) {
          @Override
          protected Object buildJsonObject(String response) {
            return response;
          }
        };
    Assertions.assertEquals(
        jsonResponseFormatter.format("Created DataSource with name test_datasource"),
        createDataSourceActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("test_datasource");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    doThrow(new RuntimeException("Error"))
        .when(dataSourceService)
        .createDataSource(dataSourceMetadata);
    CreateDataSourceActionRequest request = new CreateDataSourceActionRequest(dataSourceMetadata);
    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).createDataSource(dataSourceMetadata);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }
}
