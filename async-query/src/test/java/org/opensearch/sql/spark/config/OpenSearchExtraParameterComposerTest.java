/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.parameter.SparkSubmitParameters;

@ExtendWith(MockitoExtension.class)
class OpenSearchExtraParameterComposerTest {

  public static final String PARAMS = "PARAMS";
  @Mock SparkExecutionEngineConfigClusterSettingLoader settingsLoader;
  @Mock SparkSubmitParameters sparkSubmitParameters;
  @Mock DispatchQueryRequest dispatchQueryRequest;
  @Mock AsyncQueryRequestContext context;

  @InjectMocks OpenSearchExtraParameterComposer openSearchExtraParameterComposer;

  @Test
  public void paramExists_compose() {
    SparkExecutionEngineConfigClusterSetting setting =
        SparkExecutionEngineConfigClusterSetting.builder().sparkSubmitParameters(PARAMS).build();
    when(settingsLoader.load()).thenReturn(Optional.of(setting));

    openSearchExtraParameterComposer.compose(sparkSubmitParameters, dispatchQueryRequest, context);

    verify(sparkSubmitParameters).setExtraParameters(PARAMS);
  }

  @Test
  public void paramNotExist_compose() {
    when(settingsLoader.load()).thenReturn(Optional.empty());

    openSearchExtraParameterComposer.compose(sparkSubmitParameters, dispatchQueryRequest, context);

    verifyNoInteractions(sparkSubmitParameters);
  }
}
