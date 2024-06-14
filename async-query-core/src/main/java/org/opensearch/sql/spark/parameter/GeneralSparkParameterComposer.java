/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

/** Compose spark submit parameters. See {@link SparkParameterComposerCollection}. */
public interface GeneralSparkParameterComposer {
  void compose(
      SparkSubmitParameters sparkSubmitParameters,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context);
}
