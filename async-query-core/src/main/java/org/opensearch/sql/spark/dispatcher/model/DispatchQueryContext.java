/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;

@Getter
@Builder
public class DispatchQueryContext {
  private final String queryId;
  private final DataSourceMetadata dataSourceMetadata;
  private final Map<String, String> tags;
  private final IndexQueryDetails indexQueryDetails;
  private final AsyncQueryRequestContext asyncQueryRequestContext;
}
