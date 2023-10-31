package org.opensearch.sql.datasources.transport;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.*;
import static org.opensearch.sql.datasources.utils.XContentParserUtils.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
import org.opensearch.sql.datasources.model.transport.PatchDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.PatchDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportPatchDataSourceActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportPatchDataSourceAction action;
  @Mock private DataSourceServiceImpl dataSourceService;
  @Mock private Task task;
  @Mock private ActionListener<PatchDataSourceActionResponse> actionListener;

  @Captor
  private ArgumentCaptor<PatchDataSourceActionResponse> patchDataSourceActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;
  @Mock private OpenSearchSettings settings;

  @BeforeEach
  public void setUp() {
    action =
        new TransportPatchDataSourceAction(
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
    Map<String, Object> dataSourceData = new HashMap<>();
    dataSourceData.put(NAME_FIELD, "test_datasource");
    dataSourceData.put(DESCRIPTION_FIELD, "test");

    PatchDataSourceActionRequest request = new PatchDataSourceActionRequest(dataSourceData);

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).patchDataSource(dataSourceData);
    Mockito.verify(actionListener)
        .onResponse(patchDataSourceActionResponseArgumentCaptor.capture());
    PatchDataSourceActionResponse patchDataSourceActionResponse =
        patchDataSourceActionResponseArgumentCaptor.getValue();
    String responseAsJson = "\"Updated DataSource with name test_datasource\"";
    Assertions.assertEquals(responseAsJson, patchDataSourceActionResponse.getResult());
  }

  @Test
  public void testDoExecuteWithException() {
    Map<String, Object> dataSourceData = new HashMap<>();
    dataSourceData.put(NAME_FIELD, "test_datasource");
    dataSourceData.put(DESCRIPTION_FIELD, "test");
    doThrow(new RuntimeException("Error")).when(dataSourceService).patchDataSource(dataSourceData);
    PatchDataSourceActionRequest request = new PatchDataSourceActionRequest(dataSourceData);
    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).patchDataSource(dataSourceData);
    Mockito.verify(actionListener).onFailure(exceptionArgumentCaptor.capture());
    Exception exception = exceptionArgumentCaptor.getValue();
    Assertions.assertTrue(exception instanceof RuntimeException);
    Assertions.assertEquals("Error", exception.getMessage());
  }
}
