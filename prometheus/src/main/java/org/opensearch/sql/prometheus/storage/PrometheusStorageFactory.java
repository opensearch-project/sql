/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.datasources.utils.DatasourceValidationUtils;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.client.PrometheusClientImpl;
import org.opensearch.sql.prometheus.utils.PrometheusClientUtils;
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

  // OAuth2 constants
  public static final String OAUTH2_CLIENT_ID = "prometheus.oauth2.clientId";
  public static final String OAUTH2_CLIENT_SECRET = "prometheus.oauth2.clientSecret";
  public static final String OAUTH2_TOKEN_URL = "prometheus.oauth2.tokenUrl";

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

  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    PrometheusClient prometheusClient;
    try {
      validateDataSourceConfigProperties(requiredConfig);
      prometheusClient =
          new PrometheusClientImpl(
              PrometheusClientUtils.getHttpClient(requiredConfig, settings),
              new URI(requiredConfig.get(URI)));
    } catch (URISyntaxException | UnknownHostException e) {
      throw new IllegalArgumentException(
          String.format("Invalid URI in prometheus properties: %s", e.getMessage()));
    }
    return new PrometheusStorageEngine(prometheusClient);
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
      } else if (AuthenticationType.OAUTH2.equals(authenticationType)) {
        DatasourceValidationUtils.validateLengthAndRequiredFields(
            dataSourceMetadataConfig,
            Set.of(URI, OAUTH2_CLIENT_ID, OAUTH2_CLIENT_SECRET, OAUTH2_TOKEN_URL));
        // The token endpoint host is deliberately not deny-list checked here.
        // OAuth2TokenInterceptor validates it at request time in fetchOAuth2Token(), which is
        // the only point where the check is not open to a TOCTOU race, and it reports format
        // problems with an OAuth2-specific message. Repeating it here would swallow those
        // messages behind the generic "Invalid URI in prometheus properties" wrapper below and
        // would make data source loading depend on the token host resolving in DNS.
      }
    } else {
      DatasourceValidationUtils.validateLengthAndRequiredFields(
          dataSourceMetadataConfig, Set.of(URI));
    }
    DatasourceValidationUtils.validateHost(
        dataSourceMetadataConfig.get(URI),
        settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST));
  }
}
