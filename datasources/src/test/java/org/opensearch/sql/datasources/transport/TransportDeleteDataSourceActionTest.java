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
import org.opensearch.sql.datasources.model.transport.DeleteDataSourceActionRequest;
import org.opensearch.sql.datasources.model.transport.DeleteDataSourceActionResponse;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
public class TransportDeleteDataSourceActionTest {

  @Mock private TransportService transportService;
  @Mock private TransportDeleteDataSourceAction action;
  @Mock private DataSourceServiceImpl dataSourceService;
  @Mock private Task task;
  @Mock private ActionListener<DeleteDataSourceActionResponse> actionListener;

  @Mock private OpenSearchSettings settings;

  @Captor
  private ArgumentCaptor<DeleteDataSourceActionResponse>
      deleteDataSourceActionResponseArgumentCaptor;

  @Captor private ArgumentCaptor<Exception> exceptionArgumentCaptor;

  @BeforeEach
  public void setUp() {
    action =
        new TransportDeleteDataSourceAction(
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
    DeleteDataSourceActionRequest request = new DeleteDataSourceActionRequest("test_datasource");

    action.doExecute(task, request, actionListener);
    verify(dataSourceService, times(1)).deleteDataSource("test_datasource");
    Mockito.verify(actionListener)
        .onResponse(deleteDataSourceActionResponseArgumentCaptor.capture());
    DeleteDataSourceActionResponse deleteDataSourceActionResponse =
        deleteDataSourceActionResponseArgumentCaptor.getValue();
    Assertions.assertEquals(
        "Deleted DataSource with name test_datasource", deleteDataSourceActionResponse.getResult());
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
    Assertions.assertEquals("Error", exception.getMessage());
  }
}
