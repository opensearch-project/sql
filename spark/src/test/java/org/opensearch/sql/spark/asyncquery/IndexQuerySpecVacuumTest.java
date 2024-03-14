/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.junit.runners.Parameterized.*;
import static org.opensearch.sql.spark.flint.FlintIndexType.*;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

public class IndexQuerySpecVacuumTest extends AsyncQueryExecutorServiceSpec {

  private final List<FlintDatasetMock> flintIndices =
      Arrays.asList(
          mockDataset(
              "VACUUM SKIPPING INDEX ON mys3.default.http_logs",
              SKIPPING,
              "flint_mys3_default_http_logs_skipping_index"),
          mockDataset(
              "VACUUM INDEX test ON mys3.default.http_logs",
              COVERING,
              "flint_mys3_default_http_logs_test_index"),
          mockDataset(
              "VACUUM MATERIALIZED VIEW mys3.default.http_logs_metrics",
              MATERIALIZED_VIEW,
              "flint_mys3_default_http_logs_metrics"));

  @Test
  public void vacuumIndexWithState() {
    List<FlintIndexState> states =
        Arrays.asList(
            // FlintIndexState.ACTIVE,
            // FlintIndexState.DELETING,
            FlintIndexState.DELETED);
            // FlintIndexState.VACUUMING);
    Lists.cartesianProduct(flintIndices, states)
        .forEach(
            params -> {
              FlintDatasetMock mockDS = (FlintDatasetMock) params.get(0);
              FlintIndexState state = (FlintIndexState) params.get(1);
              String testName =
                  String.format("Vacuuming Flint index %s in %s state", mockDS.indexName, state);

              LocalEMRSClient emrsClient =
                  new LocalEMRSClient() {
                    @Override
                    public CancelJobRunResult cancelJobRun(String applicationId, String jobId) {
                      Assert.fail("should not call cancelJobRun");
                      return null;
                    }

                    @Override
                    public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
                      Assert.fail("should not call getJobRunResult");
                      return null;
                    }
                  };
              EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
              AsyncQueryExecutorService asyncQueryExecutorService =
                  createAsyncQueryExecutorService(emrServerlessClientFactory);

              // Mock flint index
              mockDS.createIndex();

              // Mock index state
              MockFlintSparkJob flintIndexJob = new MockFlintSparkJob(mockDS.latestId);
              flintIndexJob.transition(state);

              // Vacuum index
              CreateAsyncQueryResponse response =
                  asyncQueryExecutorService.createAsyncQuery(
                      new CreateAsyncQueryRequest(mockDS.query, DATASOURCE, LangType.SQL, null));

              // 2. fetch result
              assertEquals(
                  testName,
                  "SUCCESS",
                  asyncQueryExecutorService
                      .getAsyncQueryResults(response.getQueryId())
                      .getStatus());

              flintIndexJob.assertState(FlintIndexState.DELETED);
            });
  }

  private FlintDatasetMock mockDataset(String query, FlintIndexType indexType, String indexName) {
    FlintDatasetMock dataset = new FlintDatasetMock(query, "", indexType, indexName);
    dataset.latestId(Base64.getEncoder().encodeToString(indexName.getBytes()));
    return dataset;
  }
}
