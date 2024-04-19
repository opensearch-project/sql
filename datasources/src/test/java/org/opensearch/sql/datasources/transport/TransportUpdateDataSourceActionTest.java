package org.opensearch.sql.datasources.transport;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.model.transport.UpdateDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.UpdateDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportUpdateDataSourceActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportUpdateDataSourceAction action;
  @Mock private DataSourceServiceImpl dataSourceService;
  @Mock private Task task;
  @Mock private ActionListener<UpdateDataSourceActionResponse> actionListener;
  @Mock private OpenSearchSettings settings;

  @Captor
  private ArgumentCaptor<UpdateDataSourceActionResponse>
      updateDataSourceActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportUpdateDataSourceAction(
            transportService, new ActionFilters(new HashSet<>()), dataSourceService);
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
    UpdateDataSourceActionRequest request = new UpdateDataSourceActionRequest(dataSourceMetadata);

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).updateDataSource(dataSourceMetadata);
    Mockito.verify(actionListener)
        .onResponse(updateDataSourceActionResponseArgumentCaptor.capture());
    UpdateDataSourceActionResponse updateDataSourceActionResponse =
        updateDataSourceActionResponseArgumentCaptor.getValue();
    String responseAsJson = "\"Updated DataSource with name test_datasource\"";

    Assertions.assertEquals(responseAsJson, updateDataSourceActionResponse.getResult());
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
        .updateDataSource(dataSourceMetadata);
    UpdateDataSourceActionRequest request = new UpdateDataSourceActionRequest(dataSourceMetadata);
    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).updateDataSource(dataSourceMetadata);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }
}
