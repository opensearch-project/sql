package org.opensearch.sql.opensearch.prometheus;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public interface IPrometheusService {

    public String[][] queryRange(String endpoint, String query, long start, long end, int step) throws IOException;

    public static class PrometheusResponse {
        private String status;
        private ResponseData data;
        public String getStatus() {
            return status;
        }
        public void setStatus(String status) {
            this.status = status;
        }
        public ResponseData getData() {
            return data;
        }
        public void setData(ResponseData data) {
            this.data = data;
        }
    }

    public static class ResponseData {
        private String resultType;
        private List<Result> result;
        public String getResultType() {
            return resultType;
        }
        public void setResultType(String resultType) {
            this.resultType = resultType;
        }
        public List<Result> getResult() {
            return result;
        }
        public void setResult(List<Result> result) {
            this.result = result;
        }
    }
    public static class Result {
        private Metric metric;
        private String[][] values;
        public Metric getMetric() {
            return metric;
        }
        public void setMetric(Metric metric) {
            this.metric = metric;
        }
        public String[][] getValues() {
            return values;
        }
        public void setValues(String[][] values) {
            this.values = values;
        }

    }

    public static class Metric {
        @JsonProperty("__name__")
        private String name;
        private String instance;
        private String job;
        public String getName() {
            return name;
        }
        public void setName(String name) {
            this.name = name;
        }
        public String getInstance() {
            return instance;
        }
        public void setInstance(String instance) {
            this.instance = instance;
        }
        public String getJob() {
            return job;
        }
        public void setJob(String job) {
            this.job = job;
        }
    }
}