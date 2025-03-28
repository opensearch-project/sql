/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.utils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import org.opensearch.sql.common.interceptors.AwsSigningInterceptor;
import org.opensearch.sql.common.interceptors.BasicAuthenticationInterceptor;
import org.opensearch.sql.common.interceptors.URIValidatorInterceptor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasources.auth.AuthenticationType;
import org.opensearch.sql.opensearch.security.SecurityAccess;

public class PrometheusClientUtils {

  public static final String AUTH_TYPE = "prometheus.auth.type";
  public static final String USERNAME = "prometheus.auth.username";
  public static final String PASSWORD = "prometheus.auth.password";
  public static final String REGION = "prometheus.auth.region";
  public static final String ACCESS_KEY = "prometheus.auth.access_key";
  public static final String SECRET_KEY = "prometheus.auth.secret_key";

  public static OkHttpClient getHttpClient(Map<String, String> config, Settings settings) {
    return SecurityAccess.doPrivileged(
        () -> {
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
        });
  }
}
