/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

import java.util.Map;

@RequiredArgsConstructor
public class SparkStorageFactory implements DataSourceFactory {
  private final Client client;
  private final Settings settings;
  public static final String EMR_CLUSTER = "emr.cluster";
  public static final String OPENSEARCH_DOMAIN_ENDPOINT = "opensearch.domain";
  public static final String AUTH_TYPE = "emr.auth.type";
  public static final String REGION = "emr.auth.region";
  public static final String ROLE_ARN = "emr.auth.role_arn";
  public static final String ACCESS_KEY = "emr.auth.access_key";
  public static final String SECRET_KEY = "emr.auth.secret_key";

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

  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    SparkClient sparkClient;
    //TODO: Initialize spark client send to storage engine
    return new SparkStorageEngine(null);
  }
}