/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import static org.opensearch.sql.spark.data.constants.SparkConstants.EMR;

import java.security.AccessController;
import java.security.InvalidParameterException;
import java.security.PrivilegedAction;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.spark.client.EmrClientImpl;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Storage factory implementation for spark connector.
 */
@RequiredArgsConstructor
public class SparkStorageFactory implements DataSourceFactory {
  private final Client client;
  private final Settings settings;

  // Spark datasource configuration properties
  public static final String CONNECTOR_TYPE = "spark.connector";

  // EMR configuration properties
  public static final String EMR_CLUSTER = "emr.cluster";
  public static final String EMR_AUTH_TYPE = "emr.auth.type";
  public static final String EMR_REGION = "emr.auth.region";
  public static final String EMR_ROLE_ARN = "emr.auth.role_arn";
  public static final String EMR_ACCESS_KEY = "emr.auth.access_key";
  public static final String EMR_SECRET_KEY = "emr.auth.secret_key";

  // Flint integration jar configuration properties
  public static final String FLINT_HOST = "spark.datasource.flint.host";
  public static final String FLINT_PORT = "spark.datasource.flint.port";
  public static final String FLINT_SCHEME = "spark.datasource.flint.scheme";
  public static final String FLINT_AUTH = "spark.datasource.flint.auth";
  public static final String FLINT_REGION = "spark.datasource.flint.region";

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.SPARK;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.SPARK,
        getStorageEngine(metadata.getProperties()));
  }

  /**
   * This function gets spark storage engine.
   *
   * @param requiredConfig spark config options
   * @return spark storage engine object
   */
  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    SparkClient sparkClient;
    if (requiredConfig.get(CONNECTOR_TYPE).equals(EMR)) {
      sparkClient =
          AccessController.doPrivileged((PrivilegedAction<EmrClientImpl>) () -> {
            validateEMRConfigProperties(requiredConfig);
            return new EmrClientImpl(
                client,
                requiredConfig.get(EMR_CLUSTER),
                requiredConfig.get(EMR_REGION),
                requiredConfig.get(EMR_ACCESS_KEY),
                requiredConfig.get(EMR_SECRET_KEY),
                requiredConfig.get(FLINT_HOST),
                requiredConfig.get(FLINT_PORT),
                requiredConfig.get(FLINT_SCHEME),
                requiredConfig.get(FLINT_AUTH),
                requiredConfig.get(FLINT_REGION));
          });
    } else {
      throw new InvalidParameterException("Spark connector type is invalid.");
    }
    return new SparkStorageEngine(sparkClient);
  }

  private void validateEMRConfigProperties(Map<String, String> dataSourceMetadataConfig)
      throws IllegalArgumentException {
    if (dataSourceMetadataConfig.get(EMR_CLUSTER) == null
        || dataSourceMetadataConfig.get(EMR_AUTH_TYPE) == null) {
      throw new IllegalArgumentException("EMR config properties are missing.");
    } else if (dataSourceMetadataConfig.get(EMR_AUTH_TYPE)
        .equals(AuthenticationType.AWSSIGV4AUTH.getName())
        && (dataSourceMetadataConfig.get(EMR_ACCESS_KEY) == null
        || dataSourceMetadataConfig.get(EMR_SECRET_KEY) == null)) {
      throw new IllegalArgumentException("EMR auth keys are missing.");
    }
  }
}