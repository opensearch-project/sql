/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasource.model.auth.AuthenticationType;
import org.opensearch.sql.prometheus.authinterceptors.AwsSigningInterceptor;
import org.opensearch.sql.prometheus.authinterceptors.BasicAuthenticationInterceptor;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.client.PrometheusClientImpl;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

public class PrometheusStorageFactory implements DataSourceFactory {

  public static final String URI = "prometheus.uri";
  public static final String AUTH_TYPE = "prometheus.auth.type";
  public static final String USERNAME = "prometheus.auth.username";
  public static final String PASSWORD = "prometheus.auth.password";
  public static final String REGION = "prometheus.auth.region";
  public static final String ACCESS_KEY = "prometheus.auth.access_key";
  public static final String SECRET_KEY = "prometheus.auth.secret_key";

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.PROMETHEUS;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata, String clusterName) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.PROMETHEUS,
        getStorageEngine(metadata.getName(), metadata.getProperties(), clusterName));
  }

  StorageEngine getStorageEngine(String catalogName, Map<String, String> requiredConfig,
                                 String clusterName) {
    validateFieldsInConfig(requiredConfig, Set.of(URI));
    PrometheusClient prometheusClient;
    try {
      prometheusClient = new PrometheusClientImpl(getHttpClient(requiredConfig, clusterName),
          new URI(requiredConfig.get(URI)));
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Prometheus Client creation failed due to: %s", e.getMessage()));
    }
    return new PrometheusStorageEngine(prometheusClient);
  }


  private OkHttpClient getHttpClient(Map<String, String> config, String clusterName) {

    OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
    okHttpClient.callTimeout(1, TimeUnit.MINUTES);
    okHttpClient.connectTimeout(30, TimeUnit.SECONDS);
    if (config.get(AUTH_TYPE) != null) {
      AuthenticationType authenticationType = AuthenticationType.get(config.get(AUTH_TYPE));
      if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
        validateFieldsInConfig(config, Set.of(USERNAME, PASSWORD));
        okHttpClient.addInterceptor(new BasicAuthenticationInterceptor(config.get(USERNAME),
            config.get(PASSWORD)));
      } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
        validateFieldsInConfig(config, Set.of(REGION, ACCESS_KEY, SECRET_KEY));
        okHttpClient.addInterceptor(new AwsSigningInterceptor(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(config.get(ACCESS_KEY), config.get(SECRET_KEY))),
            config.get(REGION), "aps"));
      } else {
        throw new IllegalArgumentException(
            String.format("AUTH Type : %s is not supported with Prometheus Connector",
                config.get(AUTH_TYPE)));
      }
    }
    return okHttpClient.build();
  }

  private void validateFieldsInConfig(Map<String, String> config, Set<String> fields) {
    Set<String> missingFields = new HashSet<>();
    for (String field : fields) {
      if (!config.containsKey(field)) {
        missingFields.add(field);
      }
    }
    if (missingFields.size() > 0) {
      throw new IllegalArgumentException(String.format(
          "Missing %s fields in the Prometheus connector properties.", missingFields));
    }
  }


}
