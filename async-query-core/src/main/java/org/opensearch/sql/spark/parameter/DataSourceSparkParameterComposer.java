/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.parameter;

import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;

/** Compose Spark parameter based on DataSourceMetadata */
public interface DataSourceSparkParameterComposer {
  void compose(
      DataSourceMetadata dataSourceMetadata,
      SparkSubmitParameters sparkSubmitParameters,
      DispatchQueryRequest dispatchQueryRequest,
      AsyncQueryRequestContext context);
}
