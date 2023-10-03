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
import java.net.UnknownHostException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import okhttp3.OkHttpClient;
import org.opensearch.sql.common.interceptors.AwsSigningInterceptor;
import org.opensearch.sql.common.interceptors.BasicAuthenticationInterceptor;
import org.opensearch.sql.common.interceptors.URIValidatorInterceptor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.datasources.utils.DatasourceValidationUtils;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.client.PrometheusClientImpl;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

@RequiredArgsConstructor
public class PrometheusStorageFactory implements DataSourceFactory {

  public static final String URI = "prometheus.uri";
  public static final String AUTH_TYPE = "prometheus.auth.type";
  public static final String USERNAME = "prometheus.auth.username";
  public static final String PASSWORD = "prometheus.auth.password";
  public static final String REGION = "prometheus.auth.region";
  public static final String ACCESS_KEY = "prometheus.auth.access_key";
  public static final String SECRET_KEY = "prometheus.auth.secret_key";

  private final Settings settings;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.PROMETHEUS;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(), DataSourceType.PROMETHEUS, getStorageEngine(metadata.getProperties()));
  }

  // Need to refactor to a separate Validator class.
  private void validateDataSourceConfigProperties(Map<String, String> dataSourceMetadataConfig)
      throws URISyntaxException, UnknownHostException {
    if (dataSourceMetadataConfig.get(AUTH_TYPE) != null) {
      AuthenticationType authenticationType =
          AuthenticationType.get(dataSourceMetadataConfig.get(AUTH_TYPE));
      if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
        DatasourceValidationUtils.validateLengthAndRequiredFields(
            dataSourceMetadataConfig, Set.of(URI, USERNAME, PASSWORD));
      } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
        DatasourceValidationUtils.validateLengthAndRequiredFields(
            dataSourceMetadataConfig, Set.of(URI, ACCESS_KEY, SECRET_KEY, REGION));
      }
    } else {
      DatasourceValidationUtils.validateLengthAndRequiredFields(
          dataSourceMetadataConfig, Set.of(URI));
    }
    DatasourceValidationUtils.validateHost(
        dataSourceMetadataConfig.get(URI),
        settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST));
  }

  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    PrometheusClient prometheusClient;
    prometheusClient =
        AccessController.doPrivileged(
            (PrivilegedAction<PrometheusClientImpl>)
                () -> {
                  try {
                    validateDataSourceConfigProperties(requiredConfig);
                    return new PrometheusClientImpl(
                        getHttpClient(requiredConfig), new URI(requiredConfig.get(URI)));
                  } catch (URISyntaxException | UnknownHostException e) {
                    throw new IllegalArgumentException(
                        String.format("Invalid URI in prometheus properties: %s", e.getMessage()));
                  }
                });
    return new PrometheusStorageEngine(prometheusClient);
  }

  private OkHttpClient getHttpClient(Map<String, String> config) {
    OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
    okHttpClient.callTimeout(1, TimeUnit.MINUTES);
    okHttpClient.connectTimeout(30, TimeUnit.SECONDS);
    okHttpClient.followRedirects(false);
    okHttpClient.addInterceptor(
        new URIValidatorInterceptor(
            settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST)));
    if (config.get(AUTH_TYPE) != null) {
      AuthenticationType authenticationType = AuthenticationType.get(config.get(AUTH_TYPE));
      if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
        okHttpClient.addInterceptor(
            new BasicAuthenticationInterceptor(config.get(USERNAME), config.get(PASSWORD)));
      } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
        okHttpClient.addInterceptor(
            new AwsSigningInterceptor(
                new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.get(ACCESS_KEY), config.get(SECRET_KEY))),
                config.get(REGION),
                "aps"));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "AUTH Type : %s is not supported with Prometheus Connector",
                config.get(AUTH_TYPE)));
      }
    }
    return okHttpClient.build();
  }
}
