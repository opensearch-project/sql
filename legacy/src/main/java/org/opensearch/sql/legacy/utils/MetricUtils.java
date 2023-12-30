package org.opensearch.sql.legacy.utils;

import lombok.experimental.UtilityClass;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;

/** Utility to add metrics. */
@UtilityClass
public class MetricUtils {

  private static final Logger LOG = LogManager.getLogger();

  public static void incrementNumericalMetric(MetricName metricName) {
    try {
      Metrics.getInstance().getNumericalMetric(metricName).increment();
    } catch (Throwable throwable) {
      LOG.error("Error while adding metric: {}", throwable.getMessage());
    }
  }
}
