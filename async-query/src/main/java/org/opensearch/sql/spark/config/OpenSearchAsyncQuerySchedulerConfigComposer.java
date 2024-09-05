/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_EXTERNAL_SCHEDULER_ENABLED;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_JOB_EXTERNAL_SCHEDULER_INTERVAL;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.parameter.GeneralSparkParameterComposer;
import org.opensearch.sql.spark.parameter.SparkSubmitParameters;

@RequiredArgsConstructor
public class OpenSearchAsyncQuerySchedulerConfigComposer implements GeneralSparkParameterComposer {
  private final Settings settings;

  @Override
  public void compose(
      SparkSubmitParameters sparkSubmitParameters,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context) {
    Boolean externalSchedulerEnabled =
        settings.getSettingValue(Settings.Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_ENABLED);
    String externalSchedulerInterval =
        settings.getSettingValue(Settings.Key.ASYNC_QUERY_EXTERNAL_SCHEDULER_INTERVAL);
    sparkSubmitParameters.setConfigItem(
        FLINT_JOB_EXTERNAL_SCHEDULER_ENABLED, String.valueOf(externalSchedulerEnabled));
    sparkSubmitParameters.setConfigItem(
        FLINT_JOB_EXTERNAL_SCHEDULER_INTERVAL, externalSchedulerInterval);
  }
}
