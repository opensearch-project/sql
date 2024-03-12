package org.opensearch.sql.datasources.transport;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.common.setting.Settings.Key.DATASOURCES_LIMIT;

import java.util.HashSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.model.transport.CreateDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.CreateDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportCreateDataSourceActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportCreateDataSourceAction action;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private DataSourceServiceImpl dataSourceService;

  @Mock private Task task;
  @Mock private ActionListener<CreateDataSourceActionResponse> actionListener;
  @Mock private OpenSearchSettings settings;

  @Captor
  private ArgumentCaptor<CreateDataSourceActionResponse>
      createDataSourceActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportCreateDataSourceAction(
            transportService, new ActionFilters(new HashSet<>()), dataSourceService, settings);
    when(dataSourceService.getDataSourceMetadata(false).size()).thenReturn(1);
    when(settings.getSettingValue(DATASOURCES_LIMIT)).thenReturn(20);
    // Required for metrics initialization
    doReturn(emptyList()).when(settings).getSettings();
    when(settings.getSettingValue(Settings.Key.METRICS_ROLLING_INTERVAL)).thenReturn(3600L);
    when(settings.getSettingValue(Settings.Key.METRICS_ROLLING_WINDOW)).thenReturn(600L);
    LocalClusterState.state().setPluginSettings(settings);
    Metrics.getInstance().registerDefaultMetrics();
  }

  @Test
  public void testDoExecute() {
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata.Builder()
            .setName("test_datasource")
            .setConnector(DataSourceType.PROMETHEUS)
            .build();
    CreateDataSourceActionRequest request = new CreateDataSourceActionRequest(dataSourceMetadata);

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).createDataSource(dataSourceMetadata);
    Mockito.verify(actionListener)
        .onResponse(createDataSourceActionResponseArgumentCaptor.capture());
    CreateDataSourceActionResponse createDataSourceActionResponse =
        createDataSourceActionResponseArgumentCaptor.getValue();
    String responseAsJson = "\"Created DataSource with name test_datasource\"";
    Assertions.assertEquals(responseAsJson, createDataSourceActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata.Builder()
            .setName("test_datasource")
            .setConnector(DataSourceType.PROMETHEUS)
            .build();
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

  @Test
  public void testDataSourcesLimit() {
    DataSourceMetadata dataSourceMetadata =
        new DataSourceMetadata.Builder()
            .setName("test_datasource")
            .setConnector(DataSourceType.PROMETHEUS)
            .build();
    CreateDataSourceActionRequest request = new CreateDataSourceActionRequest(dataSourceMetadata);
    when(dataSourceService.getDataSourceMetadata(false).size()).thenReturn(1);
    when(settings.getSettingValue(DATASOURCES_LIMIT)).thenReturn(1);

    action.doExecute(
        task,
        request,
        new ActionListener<CreateDataSourceActionResponse>() {
          @Override
          public void onResponse(CreateDataSourceActionResponse createDataSourceActionResponse) {
            fail();
          }

          @Override
          public void onFailure(Exception e) {
            assertEquals("domain concurrent datasources can not exceed 1", e.getMessage());
          }
        });
    verify(dataSourceService, times(0)).createDataSource(dataSourceMetadata);
  }
}
