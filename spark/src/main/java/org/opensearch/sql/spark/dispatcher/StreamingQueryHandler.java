/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.INDEX_TAG_KEY;
import static org.opensearch.sql.spark.dispatcher.SparkQueryDispatcher.JOB_TYPE_TAG_KEY;

import java.util.Map;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.SparkSubmitParameters;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryContext;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryResponse;
import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;
import org.opensearch.sql.spark.dispatcher.model.JobType;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

/** Handle Streaming Query. */
public class StreamingQueryHandler extends BatchQueryHandler {
  private final EMRServerlessClient emrServerlessClient;

  public StreamingQueryHandler(
      EMRServerlessClient emrServerlessClient,
      JobExecutionResponseReader jobExecutionResponseReader) {
    super(emrServerlessClient, jobExecutionResponseReader);
    this.emrServerlessClient = emrServerlessClient;
  }

  @Override
  public DispatchQueryResponse submit(
      DispatchQueryRequest dispatchQueryRequest, DispatchQueryContext context) {
    String jobName = dispatchQueryRequest.getClusterName() + ":" + "index-query";
    IndexQueryDetails indexQueryDetails = context.getIndexQueryDetails();
    Map<String, String> tags = context.getTags();
    tags.put(INDEX_TAG_KEY, indexQueryDetails.openSearchIndexName());
    DataSourceMetadata dataSourceMetadata = context.getDataSourceMetadata();
    tags.put(JOB_TYPE_TAG_KEY, JobType.STREAMING.getText());
    StartJobRequest startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            SparkSubmitParameters.Builder.builder()
                .dataSource(dataSourceMetadata)
                .structuredStreaming(true)
                .extraParameters(dispatchQueryRequest.getExtraSparkSubmitParams())
                .build()
                .toString(),
            tags,
            indexQueryDetails.isAutoRefresh(),
            dataSourceMetadata.getResultIndex());
    String jobId = emrServerlessClient.startJobRun(startJobRequest);
    return new DispatchQueryResponse(
        AsyncQueryId.newAsyncQueryId(dataSourceMetadata.getName()),
        jobId,
        dataSourceMetadata.getResultIndex(),
        null);
  }
}
