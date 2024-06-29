/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

/**
 * Compose Spark parameters specific to the {@link
 * org.opensearch.sql.datasource.model.DataSourceType} based on the {@link DataSourceMetadata}. For
 * the parameters not specific to {@link org.opensearch.sql.datasource.model.DataSourceType}, please
 * use {@link GeneralSparkParameterComposer}.
 */
public interface DataSourceSparkParameterComposer {
  void compose(
      DataSourceMetadata dataSourceMetadata,
      SparkSubmitParameters sparkSubmitParameters,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context);
}
