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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import okhttp3.OkHttpClient;
import org.apache.commons.validator.routines.DomainValidator;
import org.opensearch.sql.common.authinterceptors.AwsSigningInterceptor;
import org.opensearch.sql.common.authinterceptors.BasicAuthenticationInterceptor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.AuthenticationType;
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
  private static final Integer MAX_LENGTH_FOR_CONFIG_PROPERTY = 1000;

  private final Settings settings;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.PROMETHEUS;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.PROMETHEUS,
        getStorageEngine(metadata.getProperties()));
  }


  //Need to refactor to a separate Validator class.
  private void validateDataSourceConfigProperties(Map<String, String> dataSourceMetadataConfig)
      throws URISyntaxException {
    if (dataSourceMetadataConfig.get(AUTH_TYPE) != null) {
      AuthenticationType authenticationType
          = AuthenticationType.get(dataSourceMetadataConfig.get(AUTH_TYPE));
      if (AuthenticationType.BASICAUTH.equals(authenticationType)) {
        validateMissingFields(dataSourceMetadataConfig, Set.of(URI, USERNAME, PASSWORD));
      } else if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
        validateMissingFields(dataSourceMetadataConfig, Set.of(URI, ACCESS_KEY, SECRET_KEY,
            REGION));
      }
    } else {
      validateMissingFields(dataSourceMetadataConfig, Set.of(URI));
    }
    validateURI(dataSourceMetadataConfig);
  }

  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    PrometheusClient prometheusClient;
    prometheusClient =
        AccessController.doPrivileged((PrivilegedAction<PrometheusClientImpl>) () -> {
          try {
            validateDataSourceConfigProperties(requiredConfig);
            return new PrometheusClientImpl(getHttpClient(requiredConfig),
                new URI(requiredConfig.get(URI)));
          } catch (URISyntaxException e) {
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

  private void validateMissingFields(Map<String, String> config, Set<String> fields) {
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

  private void validateURI(Map<String, String> config) throws URISyntaxException {
    URI uri = new URI(config.get(URI));
    String host = uri.getHost();
    if (host == null || (!(DomainValidator.getInstance().isValid(host)
        || DomainValidator.getInstance().isValidLocalTld(host)))) {
      throw new IllegalArgumentException(
          String.format("Invalid hostname in the uri: %s", config.get(URI)));
    } else {
      Pattern allowHostsPattern =
          Pattern.compile(settings.getSettingValue(Settings.Key.DATASOURCES_URI_ALLOWHOSTS));
      Matcher matcher = allowHostsPattern.matcher(host);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(
            String.format(
                "Disallowed hostname in the uri. Validate with %s config",
                Settings.Key.DATASOURCES_URI_ALLOWHOSTS.getKeyValue()));
      }
    }
  }

}
