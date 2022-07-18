package org.opensearch.sql.prometheus.config;


import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PrometheusConfig {
  //expressed in seconds
  private long defaultTimeRange = 3600;

}
