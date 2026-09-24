/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.Map;
import okhttp3.OkHttpClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.common.setting.Settings;

/*
 * @opensearch.experimental
 */
@RunWith(MockitoJUnitRunner.class)
public class PrometheusOAuth2SupportTest {

  /** A path that cannot exist, so the truststore load fails deterministically. */
  private static final String MISSING_CERT_PATH = "/nonexistent/oauth2-truststore-9f3c1a";

  @Mock private Settings settings;

  private Map<String, String> properties;

  @Before
  public void setUp() {
    properties = new HashMap<>();
    properties.put(PrometheusOAuth2Support.OAUTH2_CLIENT_ID, "prom-client");
    properties.put(PrometheusOAuth2Support.OAUTH2_CLIENT_SECRET, "prom-secret");
    properties.put(PrometheusOAuth2Support.OAUTH2_TOKEN_URL, "https://idp.example.com/token");
  }

  private void denyListIsEmpty() {
    when(settings.getSettingValue(Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST))
        .thenReturn(ImmutableList.of());
  }

  // ========== addOAuth2Interceptor ==========

  @Test
  public void testInterceptorIsAttachedToTheClient() {
    denyListIsEmpty();
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    PrometheusOAuth2Support.addOAuth2Interceptor(builder, properties, settings);

    assertEquals(1, builder.interceptors().size());
  }

  @Test
  public void testUnrelatedKeysAreNotHandedToTheInterceptor() {
    denyListIsEmpty();
    // A datasource carries basic-auth credentials and a URI alongside the OAuth2 block. None of
    // them are OAuth2 parameters, so none may reach the token request.
    properties.put("prometheus.auth.password", "basic-auth-secret");
    properties.put("prometheus.uri", "https://prometheus.example.com");
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    PrometheusOAuth2Support.addOAuth2Interceptor(builder, properties, settings);

    assertEquals(1, builder.interceptors().size());
  }

  @Test
  public void testTruststorePathReachesTheInterceptor() {
    denyListIsEmpty();
    // The truststore is stored under a prometheus-prefixed key but the shared interceptor reads a
    // connector-neutral one. Pointing at a path that cannot exist makes the load fail, which is
    // how we know the key was translated rather than filtered away with the other ssl.* keys.
    properties.put(PrometheusOAuth2Support.OAUTH2_SSL_CERT_PATH, MISSING_CERT_PATH);
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    try {
      PrometheusOAuth2Support.addOAuth2Interceptor(builder, properties, settings);
      fail("expected the unreadable truststore to be rejected");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "expected the rejected path in the message, got: " + e.getMessage(),
          e.getMessage().contains(MISSING_CERT_PATH));
    }
  }

  @Test
  public void testBlankTruststorePathIsIgnored() {
    denyListIsEmpty();
    // An operator who cleared the field leaves an empty string behind; that must mean "use the
    // JVM default truststore", not "fail to start".
    properties.put(PrometheusOAuth2Support.OAUTH2_SSL_CERT_PATH, "");
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    PrometheusOAuth2Support.addOAuth2Interceptor(builder, properties, settings);

    assertEquals(1, builder.interceptors().size());
  }

  // ========== mapOAuth2AlertmanagerConfig ==========

  @Test
  public void testAlertmanagerInheritsTheWholeBlockWhenItHasNoCredentialsOfItsOwn() {
    properties.put(PrometheusOAuth2Support.OAUTH2_SCOPES, "read");
    properties.put("prometheus.uri", "https://prometheus.example.com");
    Map<String, String> alertmanagerProperties = new HashMap<>();

    PrometheusOAuth2Support.mapOAuth2AlertmanagerConfig(properties, alertmanagerProperties);

    // The documented convenience: an Alertmanager behind the same IdP repeats nothing.
    assertEquals(
        "prom-client", alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_CLIENT_ID));
    assertEquals(
        "prom-secret", alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_CLIENT_SECRET));
    assertEquals(
        "https://idp.example.com/token",
        alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_TOKEN_URL));
    assertEquals("read", alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_SCOPES));
    // Only the OAuth2 block is inherited.
    assertNull(alertmanagerProperties.get("prometheus.uri"));
  }

  @Test
  public void testAlertmanagerWithItsOwnFullCredentialsInheritsNothing() {
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_ID, "am-client");
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_SECRET, "am-secret");
    properties.put(
        PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_TOKEN_URL, "https://other-idp.example/token");
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_SCOPES, "am-read");
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_AUDIENCE, "am-audience");
    Map<String, String> alertmanagerProperties = new HashMap<>();

    PrometheusOAuth2Support.mapOAuth2AlertmanagerConfig(properties, alertmanagerProperties);

    assertEquals("am-client", alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_CLIENT_ID));
    assertEquals(
        "am-secret", alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_CLIENT_SECRET));
    assertEquals(
        "https://other-idp.example/token",
        alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_TOKEN_URL));
    assertEquals("am-read", alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_SCOPES));
    assertEquals(
        "am-audience", alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_AUDIENCE));
  }

  @Test
  public void testClientIdAloneIsRejectedRatherThanBorrowingThePrometheusSecret() {
    // The dangerous case. Completing this from Prometheus would send the Prometheus client secret
    // out paired with a client identity it does not belong to.
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_ID, "am-client");

    assertPartialConfigRejected();
  }

  @Test
  public void testClientSecretAloneIsRejected() {
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_SECRET, "am-secret");

    assertPartialConfigRejected();
  }

  @Test
  public void testTokenUrlAloneIsRejected() {
    // Left to merge key by key, this would post the Prometheus clientId and clientSecret to
    // whichever token endpoint the Alertmanager names.
    properties.put(
        PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_TOKEN_URL, "https://other-idp.example/token");

    assertPartialConfigRejected();
  }

  @Test
  public void testCredentialsWithoutATokenUrlAreRejected() {
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_ID, "am-client");
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_SECRET, "am-secret");

    assertPartialConfigRejected();
  }

  /**
   * The message has to name Alertmanager and the inheritance rule. The interceptor's own "clientId,
   * clientSecret and tokenUrl are required" reads as simply wrong to an operator who did set
   * clientId and did set the Prometheus secret.
   */
  private void assertPartialConfigRejected() {
    Map<String, String> alertmanagerProperties = new HashMap<>();
    try {
      PrometheusOAuth2Support.mapOAuth2AlertmanagerConfig(properties, alertmanagerProperties);
      fail("expected the partial Alertmanager OAuth2 configuration to be rejected");
    } catch (IllegalArgumentException e) {
      assertTrue(
          "expected the message to name Alertmanager, got: " + e.getMessage(),
          e.getMessage().contains("Alertmanager OAuth2 configuration is incomplete"));
      assertTrue(
          "expected the message to name all three required keys, got: " + e.getMessage(),
          e.getMessage().contains(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_ID)
              && e.getMessage().contains(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_SECRET)
              && e.getMessage().contains(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_TOKEN_URL));
      assertTrue(
          "expected the message to explain that nothing is inherited, got: " + e.getMessage(),
          e.getMessage().contains("prometheus.oauth2.*"));
      // Nothing may be written on the failure path.
      assertTrue("no properties may be mapped", alertmanagerProperties.isEmpty());
    }
  }

  @Test
  public void testTruststoreIsSharedEvenWhenCredentialsAreNot() {
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_ID, "am-client");
    properties.put(PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_CLIENT_SECRET, "am-secret");
    properties.put(
        PrometheusOAuth2Support.ALERTMANAGER_OAUTH2_TOKEN_URL, "https://other-idp.example/token");
    properties.put(PrometheusOAuth2Support.OAUTH2_SSL_CERT_PATH, MISSING_CERT_PATH);
    Map<String, String> alertmanagerProperties = new HashMap<>();

    PrometheusOAuth2Support.mapOAuth2AlertmanagerConfig(properties, alertmanagerProperties);

    // The truststore describes this node, not the credentials, so it is still inherited.
    assertEquals(
        MISSING_CERT_PATH,
        alertmanagerProperties.get(PrometheusOAuth2Support.OAUTH2_SSL_CERT_PATH));
  }
}
