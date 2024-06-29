/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import lombok.RequiredArgsConstructor;

/** Provide SparkSubmitParametersBuilder instance with SparkParameterComposerCollection injected */
@RequiredArgsConstructor
public class SparkSubmitParametersBuilderProvider {
  private final SparkParameterComposerCollection sparkParameterComposerCollection;

  public SparkSubmitParametersBuilder getSparkSubmitParametersBuilder() {
    return new SparkSubmitParametersBuilder(sparkParameterComposerCollection);
  }
}
