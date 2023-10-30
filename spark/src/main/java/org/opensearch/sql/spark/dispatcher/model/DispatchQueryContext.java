/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;

@Getter
@Builder
public class DispatchQueryContext {
  private final AsyncQueryId queryId;
  private final DataSourceMetadata dataSourceMetadata;
  private final Map<String, String> tags;
  private final IndexQueryDetails indexQueryDetails;
}
