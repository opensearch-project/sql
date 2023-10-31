package org.opensearch.sql.datasources.transport;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
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
import org.opensearch.sql.datasources.model.transport.GetDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.GetDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportGetDataSourceActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportGetDataSourceAction action;
  @Mock private DataSourceServiceImpl dataSourceService;
  @Mock private Task task;
  @Mock private ActionListener<GetDataSourceActionResponse> actionListener;

  @Captor
  private ArgumentCaptor<GetDataSourceActionResponse> getDataSourceActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @Mock private OpenSearchSettings settings;

  @BeforeEach
  public void setUp() {
    action =
        new TransportGetDataSourceAction(
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
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("test_datasource");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);
    GetDataSourceActionRequest request = new GetDataSourceActionRequest("test_datasource");
    when(dataSourceService.getDataSourceMetadata("test_datasource")).thenReturn(dataSourceMetadata);

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).getDataSourceMetadata("test_datasource");
    Mockito.verify(actionListener).onResponse(getDataSourceActionResponseArgumentCaptor.capture());
    GetDataSourceActionResponse getDataSourceActionResponse =
        getDataSourceActionResponseArgumentCaptor.getValue();
    JsonResponseFormatter<DataSourceMetadata> dataSourceMetadataJsonResponseFormatter =
        new JsonResponseFormatter<>(JsonResponseFormatter.Style.PRETTY) {
          @Override
          protected Object buildJsonObject(DataSourceMetadata response) {
            return response;
          }
        };
    Assertions.assertEquals(
        dataSourceMetadataJsonResponseFormatter.format(dataSourceMetadata),
        getDataSourceActionResponse.getResult());
    DataSourceMetadata result =
        new Gson().fromJson(getDataSourceActionResponse.getResult(), DataSourceMetadata.class);
    Assertions.assertEquals("test_datasource", result.getName());
    Assertions.assertEquals(DataSourceType.PROMETHEUS, result.getConnector());
  }

  @Test
  public void testDoExecuteForGetAllDataSources() {
    DataSourceMetadata dataSourceMetadata = new DataSourceMetadata();
    dataSourceMetadata.setName("test_datasource");
    dataSourceMetadata.setConnector(DataSourceType.PROMETHEUS);

    GetDataSourceActionRequest request = new GetDataSourceActionRequest();
    when(dataSourceService.getDataSourceMetadata(false))
        .thenReturn(Collections.singleton(dataSourceMetadata));

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).getDataSourceMetadata(false);
    Mockito.verify(actionListener).onResponse(getDataSourceActionResponseArgumentCaptor.capture());
    GetDataSourceActionResponse getDataSourceActionResponse =
        getDataSourceActionResponseArgumentCaptor.getValue();
    JsonResponseFormatter<Set<DataSourceMetadata>> dataSourceMetadataJsonResponseFormatter =
        new JsonResponseFormatter<>(JsonResponseFormatter.Style.PRETTY) {
          @Override
          protected Object buildJsonObject(Set<DataSourceMetadata> response) {
            return response;
          }
        };
    Type setType = new TypeToken<Set<DataSourceMetadata>>() {}.getType();
    Assertions.assertEquals(
        dataSourceMetadataJsonResponseFormatter.format(Collections.singleton(dataSourceMetadata)),
        getDataSourceActionResponse.getResult());
    Set<DataSourceMetadata> result =
        new Gson().fromJson(getDataSourceActionResponse.getResult(), setType);
    DataSourceMetadata resultDataSource = result.iterator().next();
    Assertions.assertEquals("test_datasource", resultDataSource.getName());
    Assertions.assertEquals(DataSourceType.PROMETHEUS, resultDataSource.getConnector());
  }

  @Test
  public void testDoExecuteWithException() {
    doThrow(new RuntimeException("Error")).when(dataSourceService).getDataSourceMetadata("testDS");
    GetDataSourceActionRequest request = new GetDataSourceActionRequest("testDS");
    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).getDataSourceMetadata("testDS");
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }
}
