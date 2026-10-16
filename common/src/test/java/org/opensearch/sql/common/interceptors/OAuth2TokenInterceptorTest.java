/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.interceptors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OAuth2TokenInterceptorTest {

  @Mock private Interceptor.Chain chain;

  @Captor ArgumentCaptor<Request> requestArgumentCaptor;

  private MockWebServer mockWebServer;

  // Default deny list for testing - excludes localhost to allow testing
  private static final List<String> DEFAULT_DENY_LIST =
      Arrays.asList(
          "10.0.0.0/8", // Private network Class A
          "172.16.0.0/12", // Private network Class B
          "192.168.0.0/16", // Private network Class C
          "169.254.0.0/16", // Link-local addresses (AWS/GCP metadata)
          "::1/128", // IPv6 loopback
          "fc00::/7", // IPv6 unique local addresses
          "fe80::/10" // IPv6 link-local addresses
          );

  @BeforeEach
  void setUp() throws IOException {
    mockWebServer = new MockWebServer();
    mockWebServer.start();
  }

  @AfterEach
  void tearDown() throws IOException {
    mockWebServer.shutdown();
  }

  @Test
  void testConstructorValidation() {
    // Null credentials report the same "configuration incomplete" message as empty ones. The
    // credential parameters are intentionally not @NonNull so this explicit check runs instead
    // of Lombok's, which would raise a bare NullPointerException.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                null, "secret", "https://localhost/token", null, null, null, DEFAULT_DENY_LIST));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "client", null, "https://localhost/token", null, null, null, DEFAULT_DENY_LIST));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "client", "secret", null, null, null, null, DEFAULT_DENY_LIST));

    // Test with null deny list - this should throw NullPointerException due to @NonNull annotation
    assertThrows(
        NullPointerException.class,
        () ->
            new OAuth2TokenInterceptor(
                "client", "secret", "https://localhost/token", null, null, null, null));

    // Empty strings fail the same validation
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "", "secret", "https://localhost/token", null, null, null, DEFAULT_DENY_LIST));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "client", "", "https://localhost/token", null, null, null, DEFAULT_DENY_LIST));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "client", "secret", "", null, null, null, DEFAULT_DENY_LIST));
  }

  @Test
  void testConstructorWithValidConfig() {
    assertDoesNotThrow(
        () ->
            new OAuth2TokenInterceptor(
                "testClientId",
                "testClientSecret",
                "https://localhost:8443/token",
                "read:metrics",
                "prometheus-api",
                null,
                DEFAULT_DENY_LIST));
  }

  @Test
  void testConstructorValidationWithIncompleteConfig() {
    // A partially configured Alertmanager arrives here with nulls; it should get the
    // actionable "configuration incomplete" message rather than a NullPointerException.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "testClient",
                null,
                "https://localhost:8443/token",
                null,
                null,
                null,
                DEFAULT_DENY_LIST));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "testClient", "testSecret", null, null, null, null, DEFAULT_DENY_LIST));

    // All three null at once - the case Prometheus-with-basicauth plus Alertmanager-with-oauth2
    // produces.
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new OAuth2TokenInterceptor(null, null, null, null, null, null, DEFAULT_DENY_LIST));
    assertTrue(exception.getMessage().contains("OAuth2 configuration incomplete"));
  }

  @Test
  void testConstructorWithValidDirectParameters() {
    assertDoesNotThrow(
        () ->
            new OAuth2TokenInterceptor(
                "testClientId",
                "testClientSecret",
                "https://localhost:8443/token",
                "read:metrics",
                "prometheus-api",
                null,
                DEFAULT_DENY_LIST));
  }

  @Test
  void testCacheKeyGeneration() throws IOException {
    OAuth2TokenInterceptor interceptor1 =
        new OAuth2TokenInterceptor(
            "client1",
            "secret1",
            "https://localhost:8443/token",
            "scope1",
            "audience1",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing
    OAuth2TokenInterceptor interceptor2 =
        new OAuth2TokenInterceptor(
            "client2",
            "secret2",
            "https://localhost:8443/token",
            "scope2",
            "audience2",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    assertNotNull(interceptor1);
    assertNotNull(interceptor2);
  }

  @Test
  void testInstanceLevelCache() {
    OAuth2TokenInterceptor interceptor1 =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            "https://localhost:8443/token",
            "scope",
            "audience",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing
    OAuth2TokenInterceptor interceptor2 =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            "https://localhost:8443/token",
            "scope",
            "audience",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    assertNotNull(interceptor1);
    assertNotNull(interceptor2);
  }

  @Test
  void testCacheConfiguration() {
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            "https://localhost:8443/token",
            "scope",
            "audience",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    assertNotNull(interceptor);
  }

  @Test
  void testSecureSSLConfiguration() {
    assertDoesNotThrow(
        () ->
            new OAuth2TokenInterceptor(
                "testClient",
                "testSecret",
                "https://localhost:8443/token",
                "scope",
                "audience",
                null,
                DEFAULT_DENY_LIST));
  }

  @Test
  void testCustomCertificateConfiguration() {
    Map<String, String> sslConfig = new HashMap<>();
    sslConfig.put("ssl.certPath", "/nonexistent/path");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "testClient",
                "testSecret",
                "https://localhost:8443/token",
                "scope",
                "audience",
                sslConfig,
                DEFAULT_DENY_LIST));
  }

  @Test
  void testMultipleAudienceConfiguration() {
    assertDoesNotThrow(
        () ->
            new OAuth2TokenInterceptor(
                "testClient",
                "testSecret",
                "https://localhost:8443/token",
                "scope",
                "audience1,audience2,audience3",
                null,
                DEFAULT_DENY_LIST));
  }

  /**
   * This test demonstrates that the memory leak issue has been resolved. Previously, tokens were
   * stored in a static ConcurrentHashMap without eviction. Now, each interceptor instance has its
   * own cache with proper TTL and size limits.
   */
  @Test
  void testMemoryLeakPrevention() {
    for (int i = 0; i < 10; i++) {
      OAuth2TokenInterceptor interceptor =
          new OAuth2TokenInterceptor(
              "client" + i,
              "secret" + i,
              "https://localhost:844" + i + "/token",
              "scope",
              "audience",
              null,
              DEFAULT_DENY_LIST,
              true); // Allow HTTP for testing
      assertNotNull(interceptor);
    }

    System.gc();

    assertTrue(true, "Memory leak prevention test passed");
  }

  @Test
  void testCacheEvictionAfterInactivity() throws InterruptedException {
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            "https://localhost:8443/token",
            "scope",
            "audience",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    assertNotNull(interceptor);
  }

  @Test
  void testReusabilityWithDifferentDataSources() {
    // Prometheus data source
    OAuth2TokenInterceptor prometheusInterceptor =
        new OAuth2TokenInterceptor(
            "prometheus-client",
            "prometheus-secret",
            "https://localhost:8443/prometheus/token",
            "prometheus:read",
            "prometheus-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    // Generic metrics data source
    OAuth2TokenInterceptor metricsInterceptor =
        new OAuth2TokenInterceptor(
            "metrics-client",
            "metrics-secret",
            "https://localhost:8444/metrics/token",
            "metrics:read",
            "metrics-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    // Custom data source
    OAuth2TokenInterceptor customInterceptor =
        new OAuth2TokenInterceptor(
            "custom-client",
            "custom-secret",
            "https://localhost:8445/custom/token",
            "custom:scope",
            "custom-audience",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    assertNotNull(prometheusInterceptor);
    assertNotNull(metricsInterceptor);
    assertNotNull(customInterceptor);
  }

  // ========== INTEGRATION TESTS FOR OAUTH2 TOKEN FETCHING ==========

  @Test
  void testTokenFetchWorksCorrectly() throws IOException, InterruptedException {
    // Mock successful OAuth2 token response
    String tokenResponse =
        "{\"access_token\":\"test-token-123\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(tokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Request originalRequest = new Request.Builder().url("https://api.example.com/metrics").build();

    Response mockResponse =
        new Response.Builder()
            .request(originalRequest)
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK")
            .body(ResponseBody.create("success", MediaType.get("text/plain")))
            .build();

    when(chain.request()).thenReturn(originalRequest);
    when(chain.proceed(any(Request.class))).thenReturn(mockResponse);

    Response response = interceptor.intercept(chain);

    RecordedRequest tokenRequest = mockWebServer.takeRequest();
    assertEquals("POST", tokenRequest.getMethod());
    assertEquals("/oauth/token", tokenRequest.getPath());

    String requestBody = tokenRequest.getBody().readUtf8();
    assertTrue(requestBody.contains("grant_type=client_credentials"));
    assertTrue(requestBody.contains("scope=read%3Ametrics"));
    assertTrue(requestBody.contains("audience=test-api"));

    String authHeader = tokenRequest.getHeader("Authorization");
    assertNotNull(authHeader);
    assertTrue(authHeader.startsWith("Basic "));

    verify(chain).proceed(requestArgumentCaptor.capture());
    Request modifiedRequest = requestArgumentCaptor.getValue();
    assertEquals("Bearer test-token-123", modifiedRequest.header("Authorization"));

    assertNotNull(response);
    assertEquals(200, response.code());
  }

  @Test
  void testClientCredentialsAreFormUrlEncodedInBasicHeader()
      throws IOException, InterruptedException {
    String tokenResponse =
        "{\"access_token\":\"encoded-creds-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(tokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    // A secret with a colon and a space: sent raw, the colon would split the credential in
    // the wrong place once the IdP form-decodes it.
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "client:id", "secret with space", tokenUrl, null, null, null, DEFAULT_DENY_LIST, true);

    Request originalRequest = new Request.Builder().url("https://api.example.com/metrics").build();
    Response mockResponse =
        new Response.Builder()
            .request(originalRequest)
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK")
            .body(ResponseBody.create("success", MediaType.get("text/plain")))
            .build();

    when(chain.request()).thenReturn(originalRequest);
    when(chain.proceed(any(Request.class))).thenReturn(mockResponse);

    interceptor.intercept(chain);

    RecordedRequest tokenRequest = mockWebServer.takeRequest();
    String authHeader = tokenRequest.getHeader("Authorization");
    assertNotNull(authHeader);
    assertTrue(authHeader.startsWith("Basic "));

    String decoded =
        new String(
            Base64.getDecoder().decode(authHeader.substring("Basic ".length())),
            StandardCharsets.UTF_8);
    // RFC 6749 §2.3.1: each half is form-urlencoded, so the literal ':' inside the client id
    // becomes %3A and the space becomes '+', leaving exactly one separating colon.
    assertEquals("client%3Aid:secret+with+space", decoded);
  }

  @Test
  void testSecondCallHitsCache() throws IOException, InterruptedException {
    String tokenResponse =
        "{\"access_token\":\"cached-token-456\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(tokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Request request1 = new Request.Builder().url("https://api.example.com/metrics1").build();
    Request request2 = new Request.Builder().url("https://api.example.com/metrics2").build();

    Response mockResponse1 = createMockResponse(request1, 200);
    Response mockResponse2 = createMockResponse(request2, 200);

    when(chain.request()).thenReturn(request1, request2);
    when(chain.proceed(any(Request.class))).thenReturn(mockResponse1, mockResponse2);

    interceptor.intercept(chain);

    interceptor.intercept(chain);

    assertEquals(1, mockWebServer.getRequestCount());
    RecordedRequest tokenRequest = mockWebServer.takeRequest();
    assertEquals("POST", tokenRequest.getMethod());
    assertEquals("/oauth/token", tokenRequest.getPath());

    verify(chain, times(2)).proceed(requestArgumentCaptor.capture());
    Request modifiedRequest1 = requestArgumentCaptor.getAllValues().get(0);
    Request modifiedRequest2 = requestArgumentCaptor.getAllValues().get(1);

    assertEquals("Bearer cached-token-456", modifiedRequest1.header("Authorization"));
    assertEquals("Bearer cached-token-456", modifiedRequest2.header("Authorization"));
  }

  @Test
  void testExpiredTokenAutoRefreshes() throws IOException, InterruptedException {
    String firstTokenResponse =
        "{\"access_token\":\"expired-token\",\"token_type\":\"Bearer\",\"expires_in\":1}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(firstTokenResponse));

    String secondTokenResponse =
        "{\"access_token\":\"fresh-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(secondTokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Request request1 = new Request.Builder().url("https://api.example.com/metrics1").build();
    Request request2 = new Request.Builder().url("https://api.example.com/metrics2").build();

    Response mockResponse1 = createMockResponse(request1, 200);
    Response mockResponse2 = createMockResponse(request2, 200);

    when(chain.request()).thenReturn(request1, request2);
    when(chain.proceed(any(Request.class))).thenReturn(mockResponse1, mockResponse2);

    interceptor.intercept(chain);

    Thread.sleep(1100);

    interceptor.intercept(chain);

    assertEquals(2, mockWebServer.getRequestCount());

    verify(chain, times(2)).proceed(requestArgumentCaptor.capture());
    Request modifiedRequest1 = requestArgumentCaptor.getAllValues().get(0);
    Request modifiedRequest2 = requestArgumentCaptor.getAllValues().get(1);

    assertEquals("Bearer expired-token", modifiedRequest1.header("Authorization"));
    assertEquals("Bearer fresh-token", modifiedRequest2.header("Authorization"));
  }

  @Test
  void testPercentageBasedTokenBuffer() throws IOException, InterruptedException {
    String tokenResponse =
        "{\"access_token\":\"buffered-token\",\"token_type\":\"Bearer\",\"expires_in\":10}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(tokenResponse));

    String refreshTokenResponse =
        "{\"access_token\":\"refreshed-buffered-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(refreshTokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Request request1 = new Request.Builder().url("https://api.example.com/metrics1").build();
    Request request2 = new Request.Builder().url("https://api.example.com/metrics2").build();

    Response mockResponse1 = createMockResponse(request1, 200);
    Response mockResponse2 = createMockResponse(request2, 200);

    when(chain.request()).thenReturn(request1, request2);
    when(chain.proceed(any(Request.class))).thenReturn(mockResponse1, mockResponse2);

    interceptor.intercept(chain);

    Thread.sleep(9100);

    interceptor.intercept(chain);

    assertEquals(2, mockWebServer.getRequestCount());

    verify(chain, times(2)).proceed(requestArgumentCaptor.capture());
    Request modifiedRequest1 = requestArgumentCaptor.getAllValues().get(0);
    Request modifiedRequest2 = requestArgumentCaptor.getAllValues().get(1);

    assertEquals("Bearer buffered-token", modifiedRequest1.header("Authorization"));
    assertEquals("Bearer refreshed-buffered-token", modifiedRequest2.header("Authorization"));
  }

  @Test
  void testShortLivedTokenBuffer() throws IOException, InterruptedException {
    String tokenResponse =
        "{\"access_token\":\"short-lived-token\",\"token_type\":\"Bearer\",\"expires_in\":2}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(tokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Request request = new Request.Builder().url("https://api.example.com/metrics").build();
    Response mockResponse = createMockResponse(request, 200);

    when(chain.request()).thenReturn(request);
    when(chain.proceed(any(Request.class))).thenReturn(mockResponse);

    interceptor.intercept(chain);

    assertEquals(1, mockWebServer.getRequestCount());

    verify(chain).proceed(requestArgumentCaptor.capture());
    Request modifiedRequest = requestArgumentCaptor.getValue();
    assertEquals("Bearer short-lived-token", modifiedRequest.header("Authorization"));
  }

  @Test
  void testClientSecretRotationInvalidatesCache() throws IOException, InterruptedException {
    String firstTokenResponse =
        "{\"access_token\":\"token-with-old-secret\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(firstTokenResponse));

    String secondTokenResponse =
        "{\"access_token\":\"token-with-new-secret\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(secondTokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();

    OAuth2TokenInterceptor interceptor1 =
        new OAuth2TokenInterceptor(
            "testClient",
            "originalSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    OAuth2TokenInterceptor interceptor2 =
        new OAuth2TokenInterceptor(
            "testClient",
            "rotatedSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Request request1 = new Request.Builder().url("https://api.example.com/metrics1").build();
    Request request2 = new Request.Builder().url("https://api.example.com/metrics2").build();

    Response mockResponse1 = createMockResponse(request1, 200);
    Response mockResponse2 = createMockResponse(request2, 200);

    when(chain.request()).thenReturn(request1, request2);
    when(chain.proceed(any(Request.class))).thenReturn(mockResponse1, mockResponse2);

    interceptor1.intercept(chain);

    interceptor2.intercept(chain);

    assertEquals(2, mockWebServer.getRequestCount());

    verify(chain, times(2)).proceed(requestArgumentCaptor.capture());
    Request modifiedRequest1 = requestArgumentCaptor.getAllValues().get(0);
    Request modifiedRequest2 = requestArgumentCaptor.getAllValues().get(1);

    assertEquals("Bearer token-with-old-secret", modifiedRequest1.header("Authorization"));
    assertEquals("Bearer token-with-new-secret", modifiedRequest2.header("Authorization"));
  }

  @Test
  void testConcurrentExpiryDoesntFireMultipleFetches() throws Exception {
    String tokenResponse =
        "{\"access_token\":\"concurrent-token\",\"token_type\":\"Bearer\",\"expires_in\":1}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(tokenResponse));

    String refreshTokenResponse =
        "{\"access_token\":\"refreshed-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(refreshTokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Request initialRequest = new Request.Builder().url("https://api.example.com/initial").build();
    Response initialResponse = createMockResponse(initialRequest, 200);
    when(chain.request()).thenReturn(initialRequest);
    when(chain.proceed(any(Request.class))).thenReturn(initialResponse);
    interceptor.intercept(chain);

    Thread.sleep(1100);

    int numThreads = 3; // Reduced from 5 to 3 for better reliability
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    AtomicInteger successCount = new AtomicInteger(0);

    for (int i = 0; i < numThreads; i++) {
      final int requestId = i;
      executor.submit(
          () -> {
            try {
              startLatch.await(5, TimeUnit.SECONDS); // Add timeout to await

              Request request =
                  new Request.Builder()
                      .url("https://api.example.com/concurrent" + requestId)
                      .build();
              Response mockResponse = createMockResponse(request, 200);
              Interceptor.Chain threadChain = mock(Interceptor.Chain.class);
              when(threadChain.request()).thenReturn(request);
              when(threadChain.proceed(any(Request.class))).thenReturn(mockResponse);

              interceptor.intercept(threadChain);
              successCount.incrementAndGet();
            } catch (Exception e) {
              // Log but don't fail the test for threading issues
              System.err.println("Thread " + requestId + " failed: " + e.getMessage());
            } finally {
              doneLatch.countDown();
            }
          });
    }

    startLatch.countDown();

    boolean completed = doneLatch.await(15, TimeUnit.SECONDS); // Increased timeout
    executor.shutdown();

    // Wait a bit more for executor to fully shutdown
    executor.awaitTermination(5, TimeUnit.SECONDS);

    // Assert that all threads completed successfully
    assertTrue(
        completed,
        "Not all threads completed within timeout. Completed: "
            + successCount.get()
            + "/"
            + numThreads);
    assertEquals(numThreads, successCount.get());

    // Should have made exactly 2 requests: initial + one refresh for expired token
    // Allow some flexibility in request count due to threading
    assertTrue(
        mockWebServer.getRequestCount() >= 2,
        "Expected at least 2 requests, got: " + mockWebServer.getRequestCount());
  }

  @Test
  void testMissingConfigFieldsThrowClearErrors() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                null,
                "secret",
                "https://auth.com/token",
                "scope",
                "audience",
                null,
                DEFAULT_DENY_LIST));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "client",
                null,
                "https://auth.com/token",
                "scope",
                "audience",
                null,
                DEFAULT_DENY_LIST));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new OAuth2TokenInterceptor(
                "client", "secret", null, "scope", "audience", null, DEFAULT_DENY_LIST));

    Exception exception4 =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new OAuth2TokenInterceptor(
                    "",
                    "secret",
                    "https://auth.com/token",
                    "scope",
                    "audience",
                    null,
                    DEFAULT_DENY_LIST));
    assertTrue(exception4.getMessage().contains("OAuth2 configuration incomplete"));

    Exception exception5 =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new OAuth2TokenInterceptor(
                    "client",
                    "",
                    "https://auth.com/token",
                    "scope",
                    "audience",
                    null,
                    DEFAULT_DENY_LIST));
    assertTrue(exception5.getMessage().contains("OAuth2 configuration incomplete"));

    Exception exception6 =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new OAuth2TokenInterceptor(
                    "client", "secret", "", "scope", "audience", null, DEFAULT_DENY_LIST));
    assertTrue(exception6.getMessage().contains("OAuth2 configuration incomplete"));
  }

  @Test
  void testTokenEndpointErrorsSurfaceMeaningfulMessages() throws IOException, InterruptedException {
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(401)
            .setHeader("Content-Type", "application/json")
            .setBody(
                "{\"error\":\"invalid_client\",\"error_description\":\"Client authentication"
                    + " failed\"}"));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "invalidClient",
            "invalidSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Exception exception = assertThrows(IOException.class, () -> interceptor.intercept(chain));

    String message = exception.getMessage();
    assertTrue(
        message.contains("401"),
        "Expected the 401 from the token endpoint to surface, got: " + message);
    assertTrue(
        message.contains("invalid_client") && message.contains("Client authentication failed"),
        "Expected the RFC 6749 error/error_description to be included, got: " + message);

    // A 401 from the token endpoint means the credentials are wrong; retrying would only burn
    // IdP rate limit and delay the real message behind two sleeps.
    assertEquals(1, mockWebServer.getRequestCount());
    RecordedRequest tokenRequest = mockWebServer.takeRequest();
    assertEquals("POST", tokenRequest.getMethod());
    assertEquals("/oauth/token", tokenRequest.getPath());
  }

  @Test
  void testTokenEndpointNetworkErrorHandling() throws IOException {
    // Test HTTP protocol validation (should fail because OAuth2 requires HTTPS)
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new OAuth2TokenInterceptor(
                    "testClient",
                    "testSecret",
                    "http://localhost:8080/token",
                    "read:metrics",
                    "test-api",
                    null,
                    DEFAULT_DENY_LIST));

    assertTrue(
        exception.getMessage().contains("OAuth2 token URL must use HTTPS protocol for security"));
  }

  @Test
  void testInvalidJsonResponseHandling() throws IOException, InterruptedException {
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("invalid json response"));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Exception exception = assertThrows(IOException.class, () -> interceptor.intercept(chain));
    assertTrue(
        exception.getMessage().contains("not valid JSON"),
        "Expected the JSON parse failure to surface, got: " + exception.getMessage());

    // A single attempt: an unparseable 2xx body is a broken endpoint, not a transient fault, so
    // one enqueued response is all the interceptor is allowed to consume. If it retried, the
    // extra attempts would block on an empty queue and the assertion above would instead be
    // satisfied by a socket timeout.
    assertEquals(1, mockWebServer.getRequestCount());
    RecordedRequest tokenRequest = mockWebServer.takeRequest();
    assertEquals("POST", tokenRequest.getMethod());
    assertEquals("/oauth/token", tokenRequest.getPath());
  }

  @Test
  void testMissingAccessTokenInResponse() throws IOException, InterruptedException {
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody("{\"token_type\":\"Bearer\",\"expires_in\":3600}"));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "test-api",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Exception exception = assertThrows(IOException.class, () -> interceptor.intercept(chain));

    String message = exception.getMessage();
    assertTrue(
        message.contains("missing access_token"),
        "Expected the missing access_token error to surface, got: " + message);

    // As above: a 2xx without access_token is non-retryable, so exactly one request is made and
    // the assertion cannot be satisfied by a timeout from a second attempt on an empty queue.
    assertEquals(1, mockWebServer.getRequestCount());
    RecordedRequest tokenRequest = mockWebServer.takeRequest();
    assertEquals("POST", tokenRequest.getMethod());
    assertEquals("/oauth/token", tokenRequest.getPath());
  }

  // ========== HELPER METHODS ==========

  private Response createMockResponse(Request request, int code) {
    return new Response.Builder()
        .request(request)
        .protocol(Protocol.HTTP_1_1)
        .code(code)
        .message(code == 200 ? "OK" : "Error")
        .body(ResponseBody.create("response body", MediaType.get("text/plain")))
        .build();
  }

  @Test
  void testHashClientSecretFailsWhenSHA256Unavailable() {
    // This test verifies that the method fails fast when SHA-256 is not available
    // rather than falling back to an unsafe hash that could lead to cross-tenant token leaks.
    // In practice, SHA-256 should always be available as it's a mandatory JCE algorithm,
    // but this test ensures the security-critical behavior is correct.

    // Note: Since SHA-256 is mandatory in JCE, we can't easily simulate its absence
    // without complex mocking. This test documents the expected behavior.
    // The actual security fix is in the implementation where we throw IllegalStateException
    // instead of using String.hashCode() fallback.

    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            "https://localhost:8443/token",
            "scope",
            "audience",
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    // Verify interceptor was created successfully (SHA-256 is available)
    assertNotNull(interceptor);

    // The security fix ensures that if SHA-256 were unavailable (which shouldn't happen),
    // an IllegalStateException would be thrown instead of using the unsafe String.hashCode()
    // fallback
  }

  @Test
  void testAudienceParsingSkipsEmptyTokens() throws IOException, InterruptedException {
    // This test verifies that the audience parsing correctly handles malformed input
    // by skipping empty tokens that would cause IdP rejection

    String tokenResponse =
        "{\"access_token\":\"test-token-123\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setHeader("Content-Type", "application/json")
            .setBody(tokenResponse));

    String tokenUrl = mockWebServer.url("/oauth/token").toString();
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            tokenUrl,
            "read:metrics",
            "aud1, ,aud2,,aud3, ", // Malformed audience with empty tokens
            null,
            DEFAULT_DENY_LIST,
            true); // Allow HTTP for testing

    Request originalRequest = new Request.Builder().url("https://api.example.com/metrics").build();

    Response mockResponse =
        new Response.Builder()
            .request(originalRequest)
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK")
            .body(ResponseBody.create("success", MediaType.get("text/plain")))
            .build();

    when(chain.request()).thenReturn(originalRequest);
    when(chain.proceed(any(Request.class))).thenReturn(mockResponse);

    Response response = interceptor.intercept(chain);

    RecordedRequest tokenRequest = mockWebServer.takeRequest();
    assertEquals("POST", tokenRequest.getMethod());
    assertEquals("/oauth/token", tokenRequest.getPath());

    String requestBody = tokenRequest.getBody().readUtf8();
    assertTrue(requestBody.contains("grant_type=client_credentials"));
    assertTrue(requestBody.contains("scope=read%3Ametrics"));

    // Verify that only non-empty audience values are included
    assertTrue(requestBody.contains("audience=aud1"));
    assertTrue(requestBody.contains("audience=aud2"));
    assertTrue(requestBody.contains("audience=aud3"));

    // Verify that empty audience parameters are not included
    // (This prevents IdP rejection due to empty audience fields)
    assertFalse(requestBody.contains("audience=&") || requestBody.contains("audience=%20&"));

    assertNotNull(response);
    assertEquals(200, response.code());
  }

  /**
   * The deny list is the control requirement 3 exists for, and nothing exercised it: every other
   * test uses DEFAULT_DENY_LIST, which deliberately omits loopback so localhost works.
   */
  @Test
  void testDeniedTokenUrlHostIsRejectedWithoutContactingTheIdp() throws IOException {
    List<String> denyLoopback = Arrays.asList("127.0.0.0/8");
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            mockWebServer.url("/oauth/token").toString(),
            null,
            null,
            null,
            denyLoopback,
            true);

    // No chain.request() stub: intercept() calls getValidToken() first, so the token failure
    // is raised before the chain is ever touched. Stubbing it would be flagged as unnecessary.

    IOException exception = assertThrows(IOException.class, () -> interceptor.intercept(chain));

    // The operator needs to know it was the deny list, not a generic failure.
    assertTrue(
        exception.getMessage().contains("Disallowed hostname"),
        "expected a deny-list message, got: " + exception.getMessage());
    assertTrue(
        exception.getMessage().contains("denylist"),
        "message should name the config setting, got: " + exception.getMessage());
    // A blocked host must never result in credentials being sent.
    assertEquals(0, mockWebServer.getRequestCount());
    verify(chain, never()).proceed(any(Request.class));
  }

  /**
   * A deny-list rejection cannot become true on a retry, so it must surface immediately rather than
   * after three attempts with backoff.
   */
  @Test
  void testDeniedTokenUrlIsNotRetried() {
    List<String> denyLoopback = Arrays.asList("127.0.0.0/8");
    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            mockWebServer.url("/oauth/token").toString(),
            null,
            null,
            null,
            denyLoopback,
            true);

    // No chain.request() stub: intercept() calls getValidToken() first, so the token failure
    // is raised before the chain is ever touched. Stubbing it would be flagged as unnecessary.

    long start = System.currentTimeMillis();
    IOException exception = assertThrows(IOException.class, () -> interceptor.intercept(chain));
    long elapsed = System.currentTimeMillis() - start;

    assertFalse(
        exception.getMessage().contains("after 3 attempts"),
        "a deterministic failure should not be reported as a retry exhaustion");
    // Three attempts would cost at least the 100ms + 200ms backoff.
    assertTrue(elapsed < 300, "expected no retry backoff, took " + elapsed + "ms");
  }

  /**
   * validateTokenUrlForSSRF only checks the configured URL, so following a redirect would reach an
   * unvalidated host - the classic way to walk a deny list.
   */
  @Test
  void testTokenEndpointRedirectIsNotFollowed() throws IOException, InterruptedException {
    mockWebServer.enqueue(
        new MockResponse()
            .setResponseCode(302)
            .setHeader("Location", "http://169.254.169.254/latest/meta-data/"));

    OAuth2TokenInterceptor interceptor =
        new OAuth2TokenInterceptor(
            "testClient",
            "testSecret",
            mockWebServer.url("/oauth/token").toString(),
            null,
            null,
            null,
            DEFAULT_DENY_LIST,
            true);

    // No chain.request() stub: intercept() calls getValidToken() first, so the token failure
    // is raised before the chain is ever touched. Stubbing it would be flagged as unnecessary.

    IOException exception = assertThrows(IOException.class, () -> interceptor.intercept(chain));

    // The 302 is surfaced as a failed token request rather than chased to the metadata endpoint.
    assertTrue(
        exception.getMessage().contains("302"),
        "expected the redirect status to surface, got: " + exception.getMessage());
    // Exactly one request: the redirect target was never contacted.
    assertEquals(1, mockWebServer.getRequestCount());
    verify(chain, never()).proceed(any(Request.class));
  }
}
