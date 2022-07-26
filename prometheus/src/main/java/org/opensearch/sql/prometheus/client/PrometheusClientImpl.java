package org.opensearch.sql.prometheus.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

public class PrometheusClientImpl implements PrometheusClient {

  private static final Logger logger = LogManager.getLogger(PrometheusClientImpl.class);

  private final OkHttpClient okHttpClient;

  private final URI uri;

  public PrometheusClientImpl(OkHttpClient okHttpClient, URI uri) {
    this.okHttpClient = okHttpClient;
    this.uri = uri;
  }


  @Override
  public JSONObject queryRange(String query, long start, long end, String step) throws IOException {
    HttpUrl httpUrl = new HttpUrl.Builder()
        .scheme(uri.getScheme())
        .host(uri.getHost())
        .port(uri.getPort())
        .addPathSegment("api")
        .addPathSegment("v1")
        .addPathSegment("query_range")
        .addQueryParameter("query", query)
        .addQueryParameter("start", Long.toString(start))
        .addQueryParameter("end", Long.toString(end))
        .addQueryParameter("step", step)
        .build();
    logger.info("queryUrl: " + httpUrl);
    Request request = new Request.Builder()
        .url(httpUrl)
        .build();
    Response response = this.okHttpClient.newCall(request).execute();
    JSONObject jsonObject = new JSONObject(Objects.requireNonNull(response.body()).string());
    if ("success".equals(jsonObject.getString("status"))) {
      return jsonObject.getJSONObject("data");
    } else {
      throw new RuntimeException(jsonObject.getString("error"));
    }
  }

  @Override
  public String[] getLabels(String metricName) throws IOException {
    String queryUrl = String.format("%s/api/v1/labels?match[]=%s", uri.toString(), metricName);
    logger.info("queryUrl: " + queryUrl);
    Request request = new Request.Builder()
        .url(queryUrl)
        .build();
    Response response = this.okHttpClient.newCall(request).execute();
    ObjectMapper om = new ObjectMapper();
    PrometheusLabelResponse prometheusLabelResponse =
        om.readValue(Objects.requireNonNull(response.body()).string(),
            PrometheusLabelResponse.class);
    if ("success".equals(prometheusLabelResponse.getStatus())) {
      return prometheusLabelResponse.getData();
    }
    return null;
  }

  @Override
  public void schedule(Runnable task) {
    task.run();
  }


}
