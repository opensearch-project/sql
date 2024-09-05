package org.opensearch.sql.spark.config;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.parameter.SparkSubmitParameters;

@ExtendWith(MockitoExtension.class)
public class OpenSearchAsyncQuerySchedulerConfigComposerTest {

  @Mock private Settings settings;
  @Mock private SparkSubmitParameters sparkSubmitParameters;
  @Mock private DispatchQueryRequest dispatchQueryRequest;
  @Mock private AsyncQueryRequestContext context;

  private OpenSearchAsyncQuerySchedulerConfigComposer composer;

  @BeforeEach
  public void setUp() {
    composer = new OpenSearchAsyncQuerySchedulerConfigComposer(settings);
  }

  @Test
  public void testCompose() {
    when(settings.getSettingValue(Settings.Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED))
        .thenReturn("true");
    when(settings.getSettingValue(Settings.Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL))
        .thenReturn("10 minutes");

    composer.compose(sparkSubmitParameters, dispatchQueryRequest, context);

    verify(sparkSubmitParameters)
        .setConfigItem("spark.flint.job.externalScheduler.enabled", "true");
    verify(sparkSubmitParameters)
        .setConfigItem("spark.flint.job.externalScheduler.interval", "10 minutes");
  }

  @Test
  public void testComposeWithDisabledScheduler() {
    when(settings.getSettingValue(Settings.Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED))
        .thenReturn("false");

    composer.compose(sparkSubmitParameters, dispatchQueryRequest, context);

    verify(sparkSubmitParameters)
        .setConfigItem("spark.flint.job.externalScheduler.enabled", "false");
  }

  @Test
  public void testComposeWithMissingInterval() {
    when(settings.getSettingValue(Settings.Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED))
        .thenReturn("true");
    when(settings.getSettingValue(Settings.Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL))
        .thenReturn("");

    composer.compose(sparkSubmitParameters, dispatchQueryRequest, context);

    verify(sparkSubmitParameters).setConfigItem("spark.flint.job.externalScheduler.interval", "");
  }
}
