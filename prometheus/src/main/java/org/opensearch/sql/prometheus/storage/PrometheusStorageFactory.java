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
import java.security.AccessController;
import java.security.PrivilegedAction;
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

  private static final Integer MAX_LENGTH_FOR_CONFIG_PROPERTY = 1000;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.PROMETHEUS;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.PROMETHEUS,
        getStorageEngine(metadata.getName(), metadata.getProperties()));
  }


  private void validateDataSourceConfigProperties(Map<String, String> dataSourceMetadataConfig) {
    if (dataSourceMetadataConfig.get(AUTH_TYPE) != null) {
      AuthenticationType authenticationType
          = AuthenticationType.get(dataSourceMetadataConfig.get(AUTH_TYPE));
      if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
        validateFields(dataSourceMetadataConfig, Set.of(URI, USERNAME, PASSWORD));
      } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
        validateFields(dataSourceMetadataConfig, Set.of(URI, ACCESS_KEY, SECRET_KEY,
            REGION));
      }
    } else {
      validateFields(dataSourceMetadataConfig, Set.of(URI));
    }
  }

  StorageEngine getStorageEngine(String catalogName, Map<String, String> requiredConfig) {
    validateDataSourceConfigProperties(requiredConfig);
    PrometheusClient prometheusClient;
    prometheusClient =
        AccessController.doPrivileged((PrivilegedAction<PrometheusClientImpl>) () -> {
          try {
            return new PrometheusClientImpl(getHttpClient(requiredConfig),
                new URI(requiredConfig.get(URI)));
          } catch (URISyntaxException e) {
            throw new RuntimeException(
                String.format("Prometheus Client creation failed due to: %s", e.getMessage()));
          }
        });
    return new PrometheusStorageEngine(prometheusClient);
  }


  private OkHttpClient getHttpClient(Map<String, String> config) {
    OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
    okHttpClient.callTimeout(1, TimeUnit.MINUTES);
    okHttpClient.connectTimeout(30, TimeUnit.SECONDS);
    if (config.get(AUTH_TYPE) != null) {
      AuthenticationType authenticationType = AuthenticationType.get(config.get(AUTH_TYPE));
      if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
        okHttpClient.addInterceptor(new BasicAuthenticationInterceptor(config.get(USERNAME),
            config.get(PASSWORD)));
      } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
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

  private void validateFields(Map<String, String> config, Set<String> fields) {
    Set<String> missingFields = new HashSet<>();
    Set<String> invalidLengthFields = new HashSet<>();
    for (String field : fields) {
      if (!config.containsKey(field)) {
        missingFields.add(field);
      } else if (config.get(field).length() > MAX_LENGTH_FOR_CONFIG_PROPERTY) {
        invalidLengthFields.add(field);
      }
    }
    StringBuilder errorStringBuilder = new StringBuilder();
    if (missingFields.size() > 0) {
      errorStringBuilder.append(String.format(
          "Missing %s fields in the Prometheus connector properties.", missingFields));
    }

    if (invalidLengthFields.size() > 0) {
      errorStringBuilder.append(String.format(
          "Fields %s exceeds more than 1000 characters.", invalidLengthFields));
    }
    if (errorStringBuilder.length() > 0) {
      throw new IllegalArgumentException(errorStringBuilder.toString());
    }
  }


}
