/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.legacy;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * OpenSearch SQL integration test base class to support both security disabled and enabled OpenSearch cluster.
 */
public abstract class OpenSearchSQLRestTestCase extends OpenSearchRestTestCase {

  protected boolean isHttps() {
    boolean isHttps = Optional.ofNullable(System.getProperty("https"))
        .map("true"::equalsIgnoreCase).orElse(false);
    if (isHttps) {
      //currently only external cluster is supported for security enabled testing
      if (!Optional.ofNullable(System.getProperty("tests.rest.cluster")).isPresent()) {
        throw new RuntimeException(
            "external cluster url should be provided for security enabled testing");
      }
    }

    return isHttps;
  }

  protected String getProtocol() {
    return isHttps() ? "https" : "http";
  }

  protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
    RestClientBuilder builder = RestClient.builder(hosts);
    if (isHttps()) {
      configureHttpsClient(builder, settings, hosts[0]);
    } else {
      configureClient(builder, settings);
    }

    builder.setStrictDeprecationMode(false);
    return builder.build();
  }

  protected static void wipeAllOpenSearchIndices() throws IOException {
    // include all the indices, included hidden indices.
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html#cat-indices-api-query-params
    try {
      Response response = client().performRequest(new Request("GET", "/_cat/indices?format=json&expand_wildcards=all"));
      JSONArray jsonArray = new JSONArray(EntityUtils.toString(response.getEntity(), "UTF-8"));
      for (Object object : jsonArray) {
        JSONObject jsonObject = (JSONObject) object;
        String indexName = jsonObject.getString("index");
        //.opendistro_security isn't allowed to delete from cluster
        if (!indexName.startsWith(".opensearch_dashboards") && !indexName.startsWith(".opendistro")) {
          client().performRequest(new Request("DELETE", "/" + indexName));
        }
      }
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  protected static void configureHttpsClient(RestClientBuilder builder, Settings settings,
                                             HttpHost httpHost)
      throws IOException {
    Map<String, String> headers = ThreadContext.buildDefaultHeaders(settings);
    Header[] defaultHeaders = new Header[headers.size()];
    int i = 0;
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
    }
    builder.setDefaultHeaders(defaultHeaders);
    builder.setHttpClientConfigCallback(httpClientBuilder -> {
      String userName = Optional.ofNullable(System.getProperty("user"))
          .orElseThrow(() -> new RuntimeException("user name is missing"));
      String password = Optional.ofNullable(System.getProperty("password"))
          .orElseThrow(() -> new RuntimeException("password is missing"));
      BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider
          .setCredentials(new AuthScope(httpHost), new UsernamePasswordCredentials(userName,
              password.toCharArray()));
      try {
        final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
            .setSslContext(SSLContextBuilder.create()
                .loadTrustMaterial(null, (chains, authType) -> true)
                .build())
            .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .build();

        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            .setConnectionManager(PoolingAsyncClientConnectionManagerBuilder.create()
                .setTlsStrategy(tlsStrategy)
                .build());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    final String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
    final TimeValue socketTimeout =
        TimeValue.parseTimeValue(socketTimeoutString == null ? "60s" : socketTimeoutString,
            CLIENT_SOCKET_TIMEOUT);
    builder.setRequestConfigCallback(
        conf -> conf.setResponseTimeout(Timeout.ofMilliseconds(Math.toIntExact(socketTimeout.getMillis()))));
    if (settings.hasValue(CLIENT_PATH_PREFIX)) {
      builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
    }
  }
}
