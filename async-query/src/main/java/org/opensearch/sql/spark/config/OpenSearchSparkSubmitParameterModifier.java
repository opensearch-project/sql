/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.config;

import lombok.AllArgsConstructor;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;

@AllArgsConstructor
public class OpenSearchSparkSubmitParameterModifier implements SparkSubmitParameterModifier {

  private String extraParameters;

  @Override
  public void modifyParameters(SparkSubmitParameters parameters) {
    parameters.setExtraParameters(this.extraParameters);
  }
}
