/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.client.EMRServerlessClient;
import org.opensearch.sql.spark.execution.session.SessionManager;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.flint.FlintIndexMetadataService;
import org.opensearch.sql.spark.leasemanager.LeaseManager;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;

@RequiredArgsConstructor
public class QueryHandlerFactory {

  private final JobExecutionResponseReader jobExecutionResponseReader;
  private final FlintIndexMetadataService flintIndexMetadataService;
  private final Client client;
  private final SessionManager sessionManager;
  private final LeaseManager leaseManager;
  private final StateStore stateStore;

  public RefreshQueryHandler getRefreshQueryHandler(EMRServerlessClient emrServerlessClient) {
    return new RefreshQueryHandler(
        emrServerlessClient,
        jobExecutionResponseReader,
        flintIndexMetadataService,
        stateStore,
        leaseManager);
  }

  public StreamingQueryHandler getStreamingQueryHandler(EMRServerlessClient emrServerlessClient) {
    return new StreamingQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);
  }

  public BatchQueryHandler getBatchQueryHandler(EMRServerlessClient emrServerlessClient) {
    return new BatchQueryHandler(emrServerlessClient, jobExecutionResponseReader, leaseManager);
  }

  public InteractiveQueryHandler getInteractiveQueryHandler() {
    return new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager);
  }

  public IndexDMLHandler getIndexDMLHandler(EMRServerlessClient emrServerlessClient) {
    return new IndexDMLHandler(
        emrServerlessClient,
        jobExecutionResponseReader,
        flintIndexMetadataService,
        stateStore,
        client);
  }
}
