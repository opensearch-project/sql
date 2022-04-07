package org.opensearch.sql.opensearch.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.rest.RestHandler;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class PrometheusServiceImpl implements IPrometheusService {

    private static final Logger logger = LogManager.getLogger(PrometheusServiceImpl.class);

    private OkHttpClient okHttpClient;

    public PrometheusServiceImpl(OkHttpClient okHttpClient) {
        this.okHttpClient = okHttpClient;
    }


    @Override
    public JSONObject queryRange(String endpoint, String query, long start, long end, int step) throws IOException {
        //sample http://localhost:9090/api/v1/query_range?query=system_cpu_usage&start=1625994600&end=1626016200&step=20
        HttpUrl httpUrl = new HttpUrl.Builder()
                .scheme("http")
                .host("localhost")
                .port(9090)
                .addPathSegment("api")
                .addPathSegment("v1")
                .addPathSegment("query_range")
                .addQueryParameter("query", query)
                .addQueryParameter("start", "1649241969")
                .addQueryParameter("end", "1649242869")
                .addQueryParameter("step", "3")
                .build();
        logger.info("queryUrl: " + httpUrl.toString());
        Request request = new Request.Builder()
                .url(httpUrl)
                .build();
        Response response = this.okHttpClient.newCall(request).execute();
        JSONObject jsonObject = new JSONObject(Objects.requireNonNull(response.body()).string());
        if ("success".equals(jsonObject.getString("status"))) {
            return jsonObject.getJSONObject("data");
        }
        return null;
    }

    @Override
    public String[] getLabels(String endpoint, String metricName) throws IOException {
        String queryUrl = String.format("%s/api/v1/labels?match[]=%s", endpoint, metricName);
        logger.info("queryUrl: " + queryUrl);
        Request request = new Request.Builder()
                .url(queryUrl)
                .build();
        Response response = this.okHttpClient.newCall(request).execute();
        ObjectMapper om = new ObjectMapper();
        PrometheusLabelResponse prometheusLabelResponse = om.readValue(Objects.requireNonNull(response.body()).string(), PrometheusLabelResponse.class);
        if ("success".equals(prometheusLabelResponse.getStatus())) {
            return prometheusLabelResponse.getData();
        }
        return null;
    }


}
