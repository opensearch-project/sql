package org.opensearch.sql.opensearch.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.rest.RestHandler;

import java.io.IOException;
import java.util.Objects;

public class PrometheusServiceImpl implements IPrometheusService {

    private static final Logger logger = LogManager.getLogger(PrometheusServiceImpl.class);

    private OkHttpClient okHttpClient;

    public PrometheusServiceImpl(OkHttpClient okHttpClient) {
        this.okHttpClient = okHttpClient;
    }


    @Override
    public String[][] queryRange(String endpoint, String query, long start, long end, int step) throws IOException {
        //sample http://localhost:9090/api/v1/query_range?query=system_cpu_usage&start=1625994600&end=1626016200&step=20
        String queryUrl = String.format("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=%d", endpoint, query, start, end, step);
        logger.info("queryUrl: " + queryUrl);
        Request request = new Request.Builder()
                .url(queryUrl)
                .build();
        Response response = this.okHttpClient.newCall(request).execute();
        ObjectMapper om = new ObjectMapper();
        PrometheusResponse prometheusResponse = om.readValue(Objects.requireNonNull(response.body()).string(), PrometheusResponse.class);
        if ("success".equals(prometheusResponse.getStatus())) {
            return prometheusResponse.getData().getResult().get(0).getValues();
        }
        return null;
    }


}
