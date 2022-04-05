package org.opensearch.sql.opensearch.client;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public interface IPrometheusService {

    String[][] queryRange(String endpoint, String query, long start, long end, int step) throws IOException;


    class PrometheusResponse {
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

    @JsonIgnoreProperties
    class ResponseData {
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

    class Result {
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


    class Metric {
        @JsonProperty("__name__")
        private String name;
        private String code;

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getHandler() {
            return handler;
        }

        public void setHandler(String handler) {
            this.handler = handler;
        }

        public String getYoutube() {
            return youtube;
        }

        public void setYoutube(String youtube) {
            this.youtube = youtube;
        }

        private String handler;
        private String youtube;
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