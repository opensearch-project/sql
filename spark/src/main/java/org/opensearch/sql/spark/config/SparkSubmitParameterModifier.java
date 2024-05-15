package org.opensearch.sql.spark.config;

import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;

public interface SparkSubmitParameterModifier {
  void modifyParameters(SparkSubmitParameters parameters);
}
