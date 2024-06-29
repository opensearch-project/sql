/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.parameter.GeneralSparkParameterComposer;
import org.opensearch.sql.spark.parameter.SparkSubmitParameters;

/** Load extra parameters from settings and add to Spark submit parameters */
@RequiredArgsConstructor
public class OpenSearchExtraParameterComposer implements GeneralSparkParameterComposer {
  private final SparkExecutionEngineConfigClusterSettingLoader settingLoader;

  @Override
  public void compose(
      SparkSubmitParameters sparkSubmitParameters,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context) {
    settingLoader
        .load()
        .ifPresent(
            settings ->
                sparkSubmitParameters.setExtraParameters(settings.getSparkSubmitParameters()));
  }
}
