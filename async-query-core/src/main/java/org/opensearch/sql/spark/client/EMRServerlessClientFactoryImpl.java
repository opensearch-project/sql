/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.opensearch.sql.common.setting.Settings.Key.SPARK_EXECUTION_ENGINE_CONFIG;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.AWSEMRServerlessClientBuilder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.asyncquery.model.NullAsyncQueryRequestContext;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfig;
import org.opensearch.sql.spark.config.SparkExecutionEngineConfigSupplier;

/** Implementation of {@link EMRServerlessClientFactory}. */
@RequiredArgsConstructor
public class EMRServerlessClientFactoryImpl implements EMRServerlessClientFactory {

  private final SparkExecutionEngineConfigSupplier sparkExecutionEngineConfigSupplier;
  private EMRServerlessClient emrServerlessClient;
  private String region;

  /**
   * Gets an instance of {@link EMRServerlessClient}.
   *
   * @return An {@link EMRServerlessClient} instance.
   */
  @Override
  public EMRServerlessClient getClient() {
    SparkExecutionEngineConfig sparkExecutionEngineConfig =
        this.sparkExecutionEngineConfigSupplier.getSparkExecutionEngineConfig(
            new NullAsyncQueryRequestContext());
    validateSparkExecutionEngineConfig(sparkExecutionEngineConfig);
    if (isNewClientCreationRequired(sparkExecutionEngineConfig.getRegion())) {
      region = sparkExecutionEngineConfig.getRegion();
      this.emrServerlessClient = createEMRServerlessClient(this.region);
    }
    return this.emrServerlessClient;
  }

  private boolean isNewClientCreationRequired(String region) {
    return !region.equals(this.region);
  }

  private void validateSparkExecutionEngineConfig(
      SparkExecutionEngineConfig sparkExecutionEngineConfig) {
    if (sparkExecutionEngineConfig == null || sparkExecutionEngineConfig.getRegion() == null) {
      throw new IllegalArgumentException(
          String.format(
              "Async Query APIs are disabled. Please configure %s in cluster settings to enable"
                  + " them.",
              SPARK_EXECUTION_ENGINE_CONFIG.getKeyValue()));
    }
  }

  private EMRServerlessClient createEMRServerlessClient(String awsRegion) {
    // TODO: It does not handle accountId for now. (it creates client for same account)
    return AccessController.doPrivileged(
        (PrivilegedAction<EMRServerlessClient>)
            () -> {
              AWSEMRServerless awsemrServerless =
                  AWSEMRServerlessClientBuilder.standard()
                      .withRegion(awsRegion)
                      .withCredentials(new DefaultAWSCredentialsProviderChain())
                      .build();
              return new EmrServerlessClientImpl(awsemrServerless);
            });
  }
}
