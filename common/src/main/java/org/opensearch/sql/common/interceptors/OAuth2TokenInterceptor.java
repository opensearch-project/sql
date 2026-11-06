/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.interceptors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import lombok.NonNull;
import okhttp3.FormBody;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.URIValidationUtils;

/**
 * OAuth2 Token Interceptor that dynamically fetches and caches OAuth2 tokens for Prometheus data
 * source authentication.
 */
public class OAuth2TokenInterceptor implements Interceptor {

  private final String clientId;
  private final String clientSecret;
  private final String tokenUrl;
  private final String scopes;
  private final String audience;
  private final String grantType;
  private final OkHttpClient httpClient;
  private final java.util.List<String> denyHostList;

  private final Cache<String, TokenCache> tokenCache;

  private static final Logger LOG = LogManager.getLogger();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final long DEFAULT_TOKEN_EXPIRATION_SECONDS = 3600;
  private static final String DEFAULT_GRANT_TYPE = "client_credentials";

  /**
   * A failure that retrying cannot fix: a deny-list rejection, a malformed token URL, bad
   * credentials, or a malformed token response. Retrying these wastes ~30s of backoff before
   * reporting an obvious configuration error, and repeating invalid_client risks locking the
   * account at the IdP.
   */
  private static class NonRetryableTokenException extends IOException {
    NonRetryableTokenException(String message) {
      super(message);
    }

    NonRetryableTokenException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static class TokenCache {
    final String token;
    final Instant expiresAt;

    TokenCache(String token, long expiresInSeconds) {
      this.token = token;
      long effectiveExpiry = Math.max(1, (long) (expiresInSeconds * 0.9));
      this.expiresAt = Instant.now().plusSeconds(effectiveExpiry);
    }

    boolean isExpired() {
      return Instant.now().isAfter(expiresAt);
    }
  }

  /**
   * Constructor with direct OAuth2 parameters for better reusability.
   *
   * <p>See the delegated-to constructor for why the credential parameters are not annotated
   * {@code @NonNull}.
   *
   * @param clientId OAuth2 client ID
   * @param clientSecret OAuth2 client secret
   * @param tokenUrl OAuth2 token endpoint URL
   * @param scopes OAuth2 scopes (optional)
   * @param audience OAuth2 audience (optional)
   * @param sslConfig SSL configuration map (optional)
   * @param denyHostList List of denied host patterns for SSRF protection
   */
  public OAuth2TokenInterceptor(
      String clientId,
      String clientSecret,
      String tokenUrl,
      String scopes,
      String audience,
      Map<String, String> sslConfig,
      @NonNull java.util.List<String> denyHostList) {
    this(clientId, clientSecret, tokenUrl, scopes, audience, sslConfig, denyHostList, false);
  }

  /**
   * Constructor with direct OAuth2 parameters and test mode support.
   *
   * <p>clientId, clientSecret and tokenUrl are deliberately not annotated {@code @NonNull}: Lombok
   * injects its null check ahead of the constructor body, so an incomplete configuration would
   * surface as a bare NullPointerException instead of the message below. A partially configured
   * Alertmanager reaches this path with nulls, so the explicit check has to be the one that runs.
   *
   * @param clientId OAuth2 client ID
   * @param clientSecret OAuth2 client secret
   * @param tokenUrl OAuth2 token endpoint URL
   * @param scopes OAuth2 scopes (optional)
   * @param audience OAuth2 audience (optional)
   * @param sslConfig SSL configuration map (optional)
   * @param denyHostList List of denied host patterns for SSRF protection
   * @param allowHttpForTesting If true, allows HTTP URLs for testing purposes
   */
  public OAuth2TokenInterceptor(
      String clientId,
      String clientSecret,
      String tokenUrl,
      String scopes,
      String audience,
      Map<String, String> sslConfig,
      @NonNull java.util.List<String> denyHostList,
      boolean allowHttpForTesting) {
    this(
        clientId,
        clientSecret,
        tokenUrl,
        scopes,
        audience,
        DEFAULT_GRANT_TYPE,
        sslConfig,
        denyHostList,
        allowHttpForTesting);
  }

  /**
   * As above, but honouring a configured grant type.
   *
   * <p>OpenSearch Dashboards persists {@code prometheus.oauth2.grantType}. Hard-coding {@code
   * client_credentials} here silently downgraded any other value, so the two halves could disagree
   * about which flow was in use with nothing in the logs saying so.
   *
   * @param grantType OAuth2 grant type; defaults to client_credentials when null or empty
   */
  public OAuth2TokenInterceptor(
      String clientId,
      String clientSecret,
      String tokenUrl,
      String scopes,
      String audience,
      String grantType,
      Map<String, String> sslConfig,
      @NonNull java.util.List<String> denyHostList,
      boolean allowHttpForTesting) {

    if (clientId == null
        || clientId.isEmpty()
        || clientSecret == null
        || clientSecret.isEmpty()
        || tokenUrl == null
        || tokenUrl.isEmpty()) {
      throw new IllegalArgumentException(
          "OAuth2 configuration incomplete: clientId, clientSecret, and tokenUrl are required and"
              + " cannot be empty");
    }

    this.denyHostList = denyHostList;

    // Basic URL format validation (no DNS resolution to avoid TOCTOU)
    validateTokenUrlFormat(tokenUrl, allowHttpForTesting);

    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.tokenUrl = tokenUrl;
    this.scopes = scopes;
    this.audience = audience;
    this.grantType = (grantType == null || grantType.isEmpty()) ? DEFAULT_GRANT_TYPE : grantType;
    this.httpClient = createHttpClientWithSSLConfig(sslConfig != null ? sslConfig : Map.of());

    this.tokenCache =
        CacheBuilder.newBuilder().maximumSize(100).expireAfterAccess(1, TimeUnit.HOURS).build();
  }

  /**
   * Creates an OkHttpClient with SSL configuration that handles corporate certificates.
   *
   * @param config the configuration map
   * @return configured OkHttpClient
   * @throws IllegalArgumentException if SSL configuration fails
   */
  private OkHttpClient createHttpClientWithSSLConfig(Map<String, String> config) {
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    // validateTokenUrlForSSRF only checks the configured token URL, so a redirect would reach
    // an unvalidated host and defeat the deny list. Refuse to follow them, matching the query
    // client built in PrometheusClientUtils.getHttpClient.
    builder.followRedirects(false);
    builder.followSslRedirects(false);

    // Checking the configured URL up front is inherently TOCTOU: this client resolves the host
    // again when it dials. URIValidatorInterceptor re-checks the deny list against the request
    // OkHttp is about to make, which is the only point that cannot be raced. Matches the query
    // client in PrometheusClientUtils.getHttpClient.
    builder.addInterceptor(new URIValidatorInterceptor(denyHostList));

    String certPath = config.get("ssl.certPath");
    if (certPath != null && !certPath.isEmpty()) {
      try {
        KeyStore trustStore = loadCustomCertificates(certPath);
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, tmf.getTrustManagers(), null);

        X509TrustManager x509TrustManager = null;
        for (TrustManager tm : tmf.getTrustManagers()) {
          if (tm instanceof X509TrustManager) {
            x509TrustManager = (X509TrustManager) tm;
            break;
          }
        }

        if (x509TrustManager == null) {
          throw new IllegalArgumentException("No X509TrustManager found in trust managers");
        }

        builder.sslSocketFactory(sslContext.getSocketFactory(), x509TrustManager);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to load custom certificates from " + certPath, e);
      }
    }

    return builder.build();
  }

  /**
   * Loads custom certificates from the specified path.
   *
   * @param certPath path to certificate file or directory
   * @return KeyStore with loaded certificates
   * @throws Exception if certificates cannot be loaded
   */
  private KeyStore loadCustomCertificates(String certPath) throws Exception {
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);

    File certFile = new File(certPath);
    int certificatesLoaded = 0;

    if (certFile.isDirectory()) {
      File[] certFiles =
          certFile.listFiles(
              (dir, name) ->
                  name.toLowerCase().endsWith(".crt") || name.toLowerCase().endsWith(".pem"));

      if (certFiles != null && certFiles.length > 0) {
        for (File file : certFiles) {
          loadCertificateFromFile(trustStore, file);
          certificatesLoaded++;
        }
      }
    } else if (certFile.exists()) {
      loadCertificateFromFile(trustStore, certFile);
      certificatesLoaded++;
    } else {
      throw new Exception("Certificate path does not exist: " + certPath);
    }

    if (certificatesLoaded == 0) {
      throw new Exception("No certificates found in path: " + certPath);
    }

    return trustStore;
  }

  /**
   * Loads certificates from a file into the trust store. Handles PEM bundles with multiple
   * certificates (e.g., intermediate + root CA chains).
   *
   * @param trustStore the trust store to add the certificates to
   * @param certFile the certificate file (may contain multiple certificates)
   * @throws Exception if the certificates cannot be loaded
   */
  private void loadCertificateFromFile(KeyStore trustStore, File certFile) throws Exception {
    try (FileInputStream fis = new FileInputStream(certFile)) {
      CertificateFactory cf = CertificateFactory.getInstance("X.509");

      // Use generateCertificates (plural) to handle PEM bundles with multiple certificates
      // This is critical for corporate environments where intermediate certificates are needed
      java.util.Collection<? extends java.security.cert.Certificate> certificates =
          cf.generateCertificates(fis);

      int certIndex = 0;
      for (java.security.cert.Certificate certificate : certificates) {
        if (certificate instanceof X509Certificate) {
          X509Certificate x509Cert = (X509Certificate) certificate;
          // Use unique entry names for each certificate in the bundle
          String entryName = certFile.getName() + (certIndex == 0 ? "" : "-" + certIndex);
          trustStore.setCertificateEntry(entryName, x509Cert);
          certIndex++;
        }
      }

      if (certIndex == 0) {
        throw new Exception("No valid X.509 certificates found in file: " + certFile.getName());
      }
    }
  }

  @Override
  public Response intercept(Chain chain) throws IOException {
    Request originalRequest = chain.request();
    Response response = proceedWithToken(chain, originalRequest, getValidToken());

    // A token the IdP revoked is still inside its local TTL, so without this it would be
    // replayed on every request until that expires - up to ~24h for an expires_in of 86400,
    // and the DataSource itself is cached for 24h on the PPL path. Drop the cached token and
    // retry once with a freshly minted one. Only one retry, so a genuinely unauthorised
    // request cannot loop.
    if (response.code() == 401 || response.code() == 403) {
      response.close();
      tokenCache.invalidate(buildCacheKey());
      return proceedWithToken(chain, originalRequest, getValidToken());
    }

    return response;
  }

  private Response proceedWithToken(Chain chain, Request originalRequest, String token)
      throws IOException {
    Request authenticatedRequest =
        originalRequest.newBuilder().header("Authorization", "Bearer " + token).build();

    return chain.proceed(authenticatedRequest);
  }

  private String getValidToken() throws IOException {
    String cacheKey = buildCacheKey();

    try {
      // Cache.get(key, loader) gives single-flight behaviour: concurrent callers for the same
      // key share one token request rather than each hitting the IdP. The loader returns the
      // fully built entry so the cached lifetime is the one the IdP reported - deriving it
      // from a side-effecting put() inside the loader would depend on Guava's internal
      // ordering between that put and storeLoadedValue.
      TokenCache cached = tokenCache.get(cacheKey, this::fetchOAuth2TokenWithRetry);

      // An entry can reach its expiry between being stored and being read here. Refresh once;
      // recursing would spin without bound if the IdP kept issuing already-expired tokens.
      if (cached.isExpired()) {
        tokenCache.invalidate(cacheKey);
        cached = tokenCache.get(cacheKey, this::fetchOAuth2TokenWithRetry);
      }

      return cached.token;
    } catch (Exception e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new IOException("Failed to get valid token", e);
    }
  }

  /** Fetches an OAuth2 token with retry logic, separated from cache management. */
  private TokenCache fetchOAuth2TokenWithRetry() throws IOException {
    int maxRetries = 3;
    IOException lastException = null;

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return fetchOAuth2Token();
      } catch (NonRetryableTokenException e) {
        // Surfaced as-is: wrapping it in "after N attempts" would bury the only message that
        // tells the operator what is actually wrong.
        throw e;
      } catch (IOException e) {
        lastException = e;
        if (attempt < maxRetries) {
          try {
            // Exponential backoff: 100ms, 200ms, 400ms
            Thread.sleep(100L * (1L << (attempt - 1)));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Token fetch interrupted", ie);
          }
        }
      }
    }

    throw new IOException(
        "Failed to get valid token after " + maxRetries + " attempts", lastException);
  }

  /**
   * Validates token URL format without DNS resolution to avoid TOCTOU vulnerabilities. SSRF
   * validation is performed at request time in fetchOAuth2Token().
   */
  private void validateTokenUrlFormat(String tokenUrl, boolean allowHttpForTesting) {
    try {
      java.net.URI uri = new java.net.URI(tokenUrl);
      java.net.URL url = uri.toURL();

      // Enforce HTTPS for OAuth2 token endpoints (unless in test mode)
      if (!allowHttpForTesting && !"https".equalsIgnoreCase(url.getProtocol())) {
        throw new IllegalArgumentException(
            "OAuth2 token URL must use HTTPS protocol for security. Got: " + url.getProtocol());
      }

      // Basic format validation - no DNS resolution to avoid TOCTOU
      String host = url.getHost();
      if (host == null || host.trim().isEmpty()) {
        throw new IllegalArgumentException("OAuth2 token URL must have a valid hostname");
      }

    } catch (java.net.MalformedURLException | java.net.URISyntaxException e) {
      throw new IllegalArgumentException("Invalid OAuth2 token URL format: " + tokenUrl, e);
    }
  }

  /**
   * Validates the token URL for SSRF attacks at request time to prevent TOCTOU vulnerabilities.
   * This performs DNS resolution and validates against the deny list.
   */
  private void validateTokenUrlForSSRF(String tokenUrl) throws IOException {
    try {
      java.net.URI uri = new java.net.URI(tokenUrl);
      java.net.URL url = uri.toURL();
      String host = url.getHost();

      boolean isValidHost = URIValidationUtils.validateURIHost(host, denyHostList);
      if (!isValidHost) {
        throw new NonRetryableTokenException(
            String.format(
                "Disallowed hostname '%s' in OAuth2 token URL. This could be an SSRF attack"
                    + " attempt. Validate with %s config",
                host, Settings.Key.DATASOURCES_URI_HOSTS_DENY_LIST.getKeyValue()));
      }
    } catch (java.net.MalformedURLException | java.net.URISyntaxException e) {
      throw new NonRetryableTokenException("Invalid OAuth2 token URL format: " + tokenUrl, e);
    } catch (java.net.UnknownHostException e) {
      throw new NonRetryableTokenException(
          "Cannot resolve hostname in OAuth2 token URL: " + tokenUrl, e);
    }
  }

  private String buildCacheKey() {
    return clientId
        + ":"
        + tokenUrl
        + ":"
        + (scopes != null ? scopes : "")
        + ":"
        + (audience != null ? audience : "")
        + ":"
        + grantType
        + ":"
        + hashClientSecret(clientSecret);
  }

  /**
   * Creates a SHA-256 hash of the client secret for use in cache keys. This ensures that when
   * client secrets are rotated, cached tokens are invalidated.
   *
   * @param clientSecret the client secret to hash
   * @return SHA-256 hash of the client secret as a hex string
   * @throws IllegalStateException if SHA-256 algorithm is not available
   */
  private String hashClientSecret(String clientSecret) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(clientSecret.getBytes(StandardCharsets.UTF_8));

      StringBuilder hexString = new StringBuilder();
      for (byte b : hash) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is a mandatory JCE algorithm and should always be available.
      // Falling back to String.hashCode() would create a serious security vulnerability
      // as it has a small collision space (32-bit) that could lead to cross-tenant token leaks.
      throw new IllegalStateException(
          "SHA-256 algorithm not available. This is a critical security requirement for safe token"
              + " caching.",
          e);
    }
  }

  /** Field names only - never the values, which may include usable bearer credentials. */
  private static String fieldNames(JsonNode json) {
    java.util.List<String> names = new java.util.ArrayList<>();
    json.fieldNames().forEachRemaining(names::add);
    return String.join(",", names);
  }

  /**
   * Extracts RFC 6749 §5.2 {@code error}/{@code error_description} from a failed token response.
   * Only those two fields are read, so a body that also carries tokens cannot leak into the
   * exception message.
   */
  private static String describeError(Response response) {
    try {
      if (response.body() == null) {
        return "";
      }
      JsonNode json = OBJECT_MAPPER.readTree(response.body().string());
      String error = json.path("error").asText("");
      String description = json.path("error_description").asText("");
      if (error.isEmpty() && description.isEmpty()) {
        return "";
      }
      return " (" + error + (description.isEmpty() ? "" : ": " + description) + ")";
    } catch (Exception e) {
      // A non-JSON or unreadable error body is not itself worth failing on.
      return "";
    }
  }

  /**
   * Form-urlencodes one half of the client credentials for the HTTP Basic header.
   *
   * <p>RFC 6749 §2.3.1 requires the client id and secret to be {@code
   * application/x-www-form-urlencoded} before they are joined with a colon and base64'd. The
   * Dashboards implementation does this too; sending the raw value here meant a secret containing a
   * colon, space, '+' or '%' authenticated from Dashboards but was rejected with an opaque 401 when
   * the same data source was queried through the SQL plugin.
   *
   * <p>The five characters restored afterwards are ones {@code URLEncoder} percent-encodes while
   * the OpenSearch Dashboards implementation uses {@code encodeURIComponent}, which does not. Both
   * forms are legal and a spec-compliant IdP decodes them identically, but an IdP that compares the
   * raw header would accept a secret like {@code Xy!z(9)} through Dashboards and 401 it here.
   * Keeping the two byte-identical removes that class of asymmetry entirely.
   */
  private static String formEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8)
        .replace("%21", "!")
        .replace("%27", "'")
        .replace("%28", "(")
        .replace("%29", ")")
        .replace("%7E", "~");
  }

  private TokenCache fetchOAuth2Token() throws IOException {
    // Validate token URL for SSRF attacks at request time to prevent TOCTOU vulnerabilities
    validateTokenUrlForSSRF(tokenUrl);

    String credentials = formEncode(clientId) + ":" + formEncode(clientSecret);
    String basicAuth =
        "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));

    FormBody.Builder formBuilder = new FormBody.Builder().add("grant_type", grantType);

    if (scopes != null && !scopes.isEmpty()) {
      formBuilder.add("scope", scopes);
    }

    if (audience != null && !audience.isEmpty()) {
      // Split audience on comma and trim whitespace, but skip empty tokens to avoid
      // sending empty audience parameters that would cause IdP rejection.
      // Note: This assumes comma-separated format. For audience values containing commas,
      // consider using a List<String> parameter instead of comma-separated string.
      String[] audiences = audience.split(",");
      for (String aud : audiences) {
        String trimmedAud = aud.trim();
        if (!trimmedAud.isEmpty()) {
          formBuilder.add("audience", trimmedAud);
        }
      }
    }

    Request tokenRequest =
        new Request.Builder()
            .url(tokenUrl)
            .header("Authorization", basicAuth)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .post(formBuilder.build())
            .build();

    try (Response response = httpClient.newCall(tokenRequest).execute()) {
      if (!response.isSuccessful()) {
        // The IdP puts the actionable reason in the body (RFC 6749 §5.2 error/error_description),
        // so discarding it leaves only a bare status code to act on.
        String failure =
            "OAuth2 token request failed: "
                + response.code()
                + " "
                + response.message()
                + describeError(response);
        // Retry only what another attempt could plausibly fix: a server-side fault, or the IdP
        // explicitly asking us to come back (408, 429). A 4xx means the request itself was
        // rejected - wrong credentials, bad scope - and a 3xx means the token endpoint is
        // redirecting, which is a misconfiguration since redirects are deliberately disabled.
        // Neither changes on a retry, and retrying invalid_client risks locking the account.
        int code = response.code();
        boolean worthRetrying = code >= 500 || code == 408 || code == 429;
        if (!worthRetrying) {
          throw new NonRetryableTokenException(failure);
        }
        throw new IOException(failure);
      }

      if (response.body() == null) {
        throw new IOException("OAuth2 token response body is null");
      }

      String responseBody = response.body().string();
      JsonNode jsonResponse;
      try {
        jsonResponse = OBJECT_MAPPER.readTree(responseBody);
      } catch (JsonProcessingException e) {
        // A 2xx whose body is not JSON means we are not talking to a token endpoint at all -
        // usually a captive portal or an HTML error page from a proxy. Retrying cannot change
        // that, and JsonProcessingException extends IOException, so without this the retry loop
        // would treat it as a transient network fault and sleep through all its attempts before
        // surfacing anything.
        throw new NonRetryableTokenException(
            "OAuth2 token response was not valid JSON (HTTP " + response.code() + ")", e);
      }

      JsonNode tokenNode = jsonResponse.get("access_token");
      if (tokenNode == null || tokenNode.isNull()) {
        // Never log the raw body: an OIDC provider that returns id_token/refresh_token but no
        // access_token would put usable bearer credentials into the log store, which typically
        // has far weaker access controls than the datasource index.
        LOG.debug(
            "OAuth2 response missing access_token. Response fields: {}. error={}"
                + " error_description={}",
            fieldNames(jsonResponse),
            jsonResponse.path("error").asText(""),
            jsonResponse.path("error_description").asText(""));
        throw new NonRetryableTokenException(
            "OAuth2 response missing access_token (HTTP " + response.code() + ")");
      }
      String accessToken = tokenNode.asText();

      long expiresIn =
          jsonResponse.has("expires_in")
              ? jsonResponse.get("expires_in").asLong()
              : DEFAULT_TOKEN_EXPIRATION_SECONDS;

      // Validate expires_in to prevent issues with zero or negative values
      if (expiresIn <= 0) {
        LOG.warn("Invalid expires_in value: {}. Using default expiration.", expiresIn);
        expiresIn = DEFAULT_TOKEN_EXPIRATION_SECONDS;
      }

      // Returned rather than put() here: getValidToken stores it through the cache loader, so
      // the entry that lands in the cache is always the one carrying the IdP's own lifetime.
      return new TokenCache(accessToken, expiresIn);
    }
  }
}
