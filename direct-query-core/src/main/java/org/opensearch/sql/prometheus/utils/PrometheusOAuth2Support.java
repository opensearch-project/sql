/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.utils;

import java.util.HashMap;
import java.util.Map;
import okhttp3.OkHttpClient;
import org.opensearch.sql.common.interceptors.OAuth2TokenInterceptor;
import org.opensearch.sql.common.setting.Settings;

/**
 * OAuth2 (client credentials) support for the Prometheus connector.
 *
 * <p>Holds the OAuth2 property keys, the Alertmanager inheritance rules and the interceptor wiring,
 * so {@link PrometheusClientUtils} carries only a single dispatch call per auth type rather than
 * the details of this one.
 *
 * @opensearch.experimental
 */
public class PrometheusOAuth2Support {

  private PrometheusOAuth2Support() {}

  // Prometheus OAuth2 property keys
  public static final String OAUTH2_CLIENT_ID = "prometheus.oauth2.clientId";
  public static final String OAUTH2_CLIENT_SECRET = "prometheus.oauth2.clientSecret";
  public static final String OAUTH2_TOKEN_URL = "prometheus.oauth2.tokenUrl";
  public static final String OAUTH2_SCOPES = "prometheus.oauth2.scopes";
  public static final String OAUTH2_AUDIENCE = "prometheus.oauth2.audience";
  public static final String OAUTH2_GRANT_TYPE = "prometheus.oauth2.grantType";
  public static final String OAUTH2_PREFIX = "prometheus.oauth2.";
  public static final String OAUTH2_SSL_PREFIX = "prometheus.oauth2.ssl.";
  public static final String OAUTH2_SSL_CERT_PATH = "prometheus.oauth2.ssl.certPath";

  // AlertManager OAuth2 property keys
  public static final String ALERTMANAGER_OAUTH2_CLIENT_ID = "alertmanager.oauth2.clientId";
  public static final String ALERTMANAGER_OAUTH2_CLIENT_SECRET = "alertmanager.oauth2.clientSecret";
  public static final String ALERTMANAGER_OAUTH2_TOKEN_URL = "alertmanager.oauth2.tokenUrl";
  public static final String ALERTMANAGER_OAUTH2_SCOPES = "alertmanager.oauth2.scopes";
  public static final String ALERTMANAGER_OAUTH2_AUDIENCE = "alertmanager.oauth2.audience";
  public static final String ALERTMANAGER_OAUTH2_GRANT_TYPE = "alertmanager.oauth2.grantType";

  /**
   * Adds the OAuth2 bearer token interceptor to a Prometheus or Alertmanager HTTP client.
   *
   * @param builder the client being configured
   * @param config the data source properties
   * @param settings used to read the URI host deny list for SSRF protection
   */
  public static void addOAuth2Interceptor(
      OkHttpClient.Builder builder, Map<String, String> config, Settings settings) {
    Map<String, String> oauth2Config = filterOAuth2Config(config);
    builder.addInterceptor(
        new OAuth2TokenInterceptor(
            oauth2Config.get(OAUTH2_CLIENT_ID),
            oauth2Config.get(OAUTH2_CLIENT_SECRET),
            oauth2Config.get(OAUTH2_TOKEN_URL),
            oauth2Config.get(OAUTH2_SCOPES),
            oauth2Config.get(OAUTH2_AUDIENCE),
            // Dashboards persists this and honours it in its own token request. Taking the
            // 7-argument constructor here silently forced client_credentials, so a data source
            // configured for another flow worked from Dashboards and used the wrong grant from
            // here, with nothing in the logs saying so. The interceptor falls back to
            // client_credentials when this is null or empty.
            oauth2Config.get(OAUTH2_GRANT_TYPE),
            filterSSLConfig(config),
            settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST),
            false));
  }

  /**
   * Maps the {@code alertmanager.oauth2.*} properties onto the {@code prometheus.oauth2.*} keys the
   * interceptor reads.
   *
   * <p>Inheritance is all or nothing. An Alertmanager that names none of clientId, clientSecret or
   * tokenUrl inherits the whole Prometheus OAuth2 block, so a deployment behind the same IdP does
   * not have to repeat the configuration. As soon as it names any of them it is treated as having
   * its own identity and inherits no credentials at all - only the truststore, which describes this
   * node rather than the credentials. A partial override is rejected rather than completed from the
   * Prometheus block, which would pair the Prometheus client secret with a different client id.
   *
   * @param properties the data source properties
   * @param alertmanagerProperties the Alertmanager properties map to populate
   * @throws IllegalArgumentException if the Alertmanager names some but not all of clientId,
   *     clientSecret and tokenUrl
   */
  public static void mapOAuth2AlertmanagerConfig(
      Map<String, String> properties, Map<String, String> alertmanagerProperties) {
    // Credentials are inherited as a unit, never key by key. Merging them would let an
    // Alertmanager that sets only alertmanager.oauth2.clientId pair that id with the Prometheus
    // clientSecret - an unexplained 401, and the Prometheus secret sent out under a client
    // identity it does not belong to.
    boolean hasOwnCredentials =
        properties.containsKey(ALERTMANAGER_OAUTH2_CLIENT_ID)
            || properties.containsKey(ALERTMANAGER_OAUTH2_CLIENT_SECRET)
            || properties.containsKey(ALERTMANAGER_OAUTH2_TOKEN_URL);

    if (hasOwnCredentials) {
      // Partial is a configuration error. Reported here rather than left to the interceptor,
      // whose "clientId, clientSecret and tokenUrl are required" reads as wrong to an operator
      // who did set clientId and did set the Prometheus secret - it names neither Alertmanager
      // nor the inheritance rule that stopped applying.
      boolean complete =
          properties.containsKey(ALERTMANAGER_OAUTH2_CLIENT_ID)
              && properties.containsKey(ALERTMANAGER_OAUTH2_CLIENT_SECRET)
              && properties.containsKey(ALERTMANAGER_OAUTH2_TOKEN_URL);
      if (!complete) {
        throw new IllegalArgumentException(
            "Alertmanager OAuth2 configuration is incomplete: "
                + ALERTMANAGER_OAUTH2_CLIENT_ID
                + ", "
                + ALERTMANAGER_OAUTH2_CLIENT_SECRET
                + " and "
                + ALERTMANAGER_OAUTH2_TOKEN_URL
                + " must be set together. Setting any of them means the Alertmanager has its own"
                + " client identity, so none of them are inherited from the prometheus.oauth2.*"
                + " block - that would pair the Prometheus client secret with a different client"
                + " id. Either set all three, or set none of them to reuse the Prometheus"
                + " credentials.");
      }

      copyIfPresent(
          properties, alertmanagerProperties, ALERTMANAGER_OAUTH2_CLIENT_ID, OAUTH2_CLIENT_ID);
      copyIfPresent(
          properties,
          alertmanagerProperties,
          ALERTMANAGER_OAUTH2_CLIENT_SECRET,
          OAUTH2_CLIENT_SECRET);
      copyIfPresent(
          properties, alertmanagerProperties, ALERTMANAGER_OAUTH2_TOKEN_URL, OAUTH2_TOKEN_URL);
      copyIfPresent(properties, alertmanagerProperties, ALERTMANAGER_OAUTH2_SCOPES, OAUTH2_SCOPES);
      copyIfPresent(
          properties, alertmanagerProperties, ALERTMANAGER_OAUTH2_AUDIENCE, OAUTH2_AUDIENCE);
      copyIfPresent(
          properties, alertmanagerProperties, ALERTMANAGER_OAUTH2_GRANT_TYPE, OAUTH2_GRANT_TYPE);
      // Only the truststore is shared, since it is a property of this node rather than of the
      // credentials.
      copyIfPresent(properties, alertmanagerProperties, OAUTH2_SSL_CERT_PATH, OAUTH2_SSL_CERT_PATH);
      return;
    }

    // No Alertmanager-specific credentials at all: inherit the whole Prometheus OAuth2 block,
    // which is the documented convenience for an Alertmanager behind the same IdP.
    properties.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(OAUTH2_PREFIX))
        .forEach(entry -> alertmanagerProperties.putIfAbsent(entry.getKey(), entry.getValue()));
  }

  private static void copyIfPresent(
      Map<String, String> from, Map<String, String> to, String fromKey, String toKey) {
    if (from.containsKey(fromKey)) {
      to.put(toKey, from.get(fromKey));
    }
  }

  /**
   * Narrows the configuration to the OAuth2 keys, so unrelated secrets are never handed to the
   * interceptor. SSL keys are excluded here and translated separately by {@link #filterSSLConfig}.
   */
  private static Map<String, String> filterOAuth2Config(Map<String, String> config) {
    Map<String, String> oauth2Config = new HashMap<>();

    config.entrySet().stream()
        .filter(entry -> entry.getKey().startsWith(OAUTH2_PREFIX))
        .filter(entry -> !entry.getKey().startsWith(OAUTH2_SSL_PREFIX))
        .forEach(entry -> oauth2Config.put(entry.getKey(), entry.getValue()));

    return oauth2Config;
  }

  /**
   * Translates the Prometheus-specific SSL keys into the neutral keys the shared interceptor reads,
   * keeping connector naming out of it.
   */
  private static Map<String, String> filterSSLConfig(Map<String, String> config) {
    Map<String, String> sslConfig = new HashMap<>();

    String certPath = config.get(OAUTH2_SSL_CERT_PATH);
    if (certPath != null) {
      sslConfig.put("ssl.certPath", certPath);
    }

    return sslConfig;
  }
}
