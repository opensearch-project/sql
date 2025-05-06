package org.opensearch.sql.spark.config;

import org.opensearch.sql.spark.parameter.SparkSubmitParametersBuilder;

/**
 * Interface for extension point to allow modification of spark submit parameter. modifyParameter
 * method is called after the default spark submit parameter is build. To be deprecated in favor of
 * {@link org.opensearch.sql.spark.parameter.GeneralSparkParameterComposer}
 */
public interface SparkSubmitParameterModifier {
  void modifyParameters(SparkSubmitParametersBuilder parametersBuilder);
}
