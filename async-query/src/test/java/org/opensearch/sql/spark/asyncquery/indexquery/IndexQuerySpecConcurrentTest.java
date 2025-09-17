/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.indexquery;

import org.junit.Test;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.model.MockFlintSparkJob;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.leasemanager.ConcurrencyLimitExceededException;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class IndexQuerySpecConcurrentTest extends IndexQuerySpecBase {
  @Test
  public void concurrentRefreshJobLimitNotApplied() {
    EMRServerlessClientFactory emrServerlessClientFactory = new LocalEMRServerlessClientFactory();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // Mock flint index
    COVERING.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob =
        new MockFlintSparkJob(flintIndexStateModelService, COVERING.latestId, MYS3_DATASOURCE);
    flintIndexJob.refreshing();

    // query with auto refresh
    String query =
        "CREATE INDEX covering ON mys3.default.http_logs(l_orderkey, "
            + "l_quantity) WITH (auto_refresh = true)";
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(query, MYS3_DATASOURCE, LangType.SQL, null),
            asyncQueryRequestContext);
    assertNull(response.getSessionId());
  }

  @Test
  public void concurrentRefreshJobLimitAppliedToDDLWithAuthRefresh() {
    EMRServerlessClientFactory emrServerlessClientFactory = new LocalEMRServerlessClientFactory();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    setConcurrentRefreshJob(1);

    // Mock flint index
    COVERING.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob =
        new MockFlintSparkJob(flintIndexStateModelService, COVERING.latestId, MYS3_DATASOURCE);
    flintIndexJob.refreshing();

    // query with auto_refresh = true.
    String query =
        "CREATE INDEX covering ON mys3.default.http_logs(l_orderkey, "
            + "l_quantity) WITH (auto_refresh = true)";
    ConcurrencyLimitExceededException exception =
        assertThrows(
            ConcurrencyLimitExceededException.class,
            () ->
                asyncQueryExecutorService.createAsyncQuery(
                    new CreateAsyncQueryRequest(query, MYS3_DATASOURCE, LangType.SQL, null),
                    asyncQueryRequestContext));
    assertEquals("domain concurrent refresh job can not exceed 1", exception.getMessage());
  }

  @Test
  public void concurrentRefreshJobLimitAppliedToRefresh() {
    EMRServerlessClientFactory emrServerlessClientFactory = new LocalEMRServerlessClientFactory();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    setConcurrentRefreshJob(1);

    // Mock flint index
    COVERING.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob =
        new MockFlintSparkJob(flintIndexStateModelService, COVERING.latestId, MYS3_DATASOURCE);
    flintIndexJob.refreshing();

    // query with auto_refresh = true.
    String query = "REFRESH INDEX covering ON mys3.default.http_logs";
    ConcurrencyLimitExceededException exception =
        assertThrows(
            ConcurrencyLimitExceededException.class,
            () ->
                asyncQueryExecutorService.createAsyncQuery(
                    new CreateAsyncQueryRequest(query, MYS3_DATASOURCE, LangType.SQL, null),
                    asyncQueryRequestContext));
    assertEquals("domain concurrent refresh job can not exceed 1", exception.getMessage());
  }

  @Test
  public void concurrentRefreshJobLimitNotAppliedToDDL() {
    String query = "CREATE INDEX covering ON mys3.default.http_logs(l_orderkey, l_quantity)";
    EMRServerlessClientFactory emrServerlessClientFactory = new LocalEMRServerlessClientFactory();
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    setConcurrentRefreshJob(1);

    // Mock flint index
    COVERING.createIndex();
    // Mock index state
    MockFlintSparkJob flintIndexJob =
        new MockFlintSparkJob(flintIndexStateModelService, COVERING.latestId, MYS3_DATASOURCE);
    flintIndexJob.refreshing();

    CreateAsyncQueryResponse asyncQueryResponse =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(query, MYS3_DATASOURCE, LangType.SQL, null),
            asyncQueryRequestContext);
  }
}
