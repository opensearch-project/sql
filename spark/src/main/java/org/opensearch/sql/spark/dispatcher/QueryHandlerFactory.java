/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
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
  private final EMRServerlessClientFactory emrServerlessClientFactory;

  public RefreshQueryHandler getRefreshQueryHandler() {
    return new RefreshQueryHandler(
        emrServerlessClientFactory.getClient(),
        jobExecutionResponseReader,
        flintIndexMetadataService,
        stateStore,
        leaseManager);
  }

  public StreamingQueryHandler getStreamingQueryHandler() {
    return new StreamingQueryHandler(
        emrServerlessClientFactory.getClient(), jobExecutionResponseReader, leaseManager);
  }

  public BatchQueryHandler getBatchQueryHandler() {
    return new BatchQueryHandler(
        emrServerlessClientFactory.getClient(), jobExecutionResponseReader, leaseManager);
  }

  public InteractiveQueryHandler getInteractiveQueryHandler() {
    return new InteractiveQueryHandler(sessionManager, jobExecutionResponseReader, leaseManager);
  }

  public IndexDMLHandler getIndexDMLHandler() {
    return new IndexDMLHandler(
        emrServerlessClientFactory.getClient(),
        jobExecutionResponseReader,
        flintIndexMetadataService,
        stateStore,
        client);
  }
}
