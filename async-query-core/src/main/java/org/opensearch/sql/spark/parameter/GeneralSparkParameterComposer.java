/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

/**
 * Compose spark submit parameters based on the request and context. For {@link
 * org.opensearch.sql.datasource.model.DataSourceType} specific parameters, please use {@link
 * DataSourceSparkParameterComposer}. See {@link SparkParameterComposerCollection}.
 */
public interface GeneralSparkParameterComposer {

  /**
   * Modify sparkSubmitParameters based on dispatchQueryRequest and context.
   *
   * @param sparkSubmitParameters Implementation of this method will modify this.
   * @param dispatchQueryRequest Request. Implementation can refer it to compose
   *     sparkSubmitParameters.
   * @param context Context of the request. Implementation can refer it to compose
   *     sparkSubmitParameters.
   */
  void compose(
      SparkSubmitParameters sparkSubmitParameters,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context);
}
