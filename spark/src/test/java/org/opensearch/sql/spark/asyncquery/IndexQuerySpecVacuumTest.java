/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.spark.execution.statestore.StateStore.DATASOURCE_TO_REQUEST_INDEX;
import static org.opensearch.sql.spark.flint.FlintIndexState.ACTIVE;
import static org.opensearch.sql.spark.flint.FlintIndexState.CREATING;
import static org.opensearch.sql.spark.flint.FlintIndexState.DELETED;
import static org.opensearch.sql.spark.flint.FlintIndexState.EMPTY;
import static org.opensearch.sql.spark.flint.FlintIndexState.REFRESHING;
import static org.opensearch.sql.spark.flint.FlintIndexState.VACUUMING;
import static org.opensearch.sql.spark.flint.FlintIndexType.COVERING;
import static org.opensearch.sql.spark.flint.FlintIndexType.MATERIALIZED_VIEW;
import static org.opensearch.sql.spark.flint.FlintIndexType.SKIPPING;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.google.common.collect.Lists;
import java.util.Base64;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.MockFlintSparkJob;
import org.opensearch.sql.spark.client.EMRServerlessClientFactory;
import org.opensearch.sql.spark.flint.FlintIndexState;
import org.opensearch.sql.spark.flint.FlintIndexType;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryResponse;
import org.opensearch.sql.spark.rest.model.LangType;

@SuppressWarnings({"unchecked", "rawtypes"})
public class IndexQuerySpecVacuumTest extends AsyncQueryExecutorServiceSpec {

  private static final EMRApiCall DEFAULT_OP = () -> null;

  private final List<FlintDatasetMock> FLINT_TEST_DATASETS =
      List.of(
          mockDataset(
              "VACUUM SKIPPING INDEX ON mys3.default.http_logs",
              SKIPPING,
              "flint_mys3_default_http_logs_skipping_index"),
          mockDataset(
              "VACUUM INDEX covering ON mys3.default.http_logs",
              COVERING,
              "flint_mys3_default_http_logs_covering_index"),
          mockDataset(
              "VACUUM MATERIALIZED VIEW mys3.default.http_logs_metrics",
              MATERIALIZED_VIEW,
              "flint_mys3_default_http_logs_metrics"),
          mockDataset(
                  "VACUUM SKIPPING INDEX ON mys3.default.`test ,:\"+/\\|?#><`",
                  SKIPPING,
                  "flint_mys3_default_test%20%2c%3a%22%2b%2f%5c%7c%3f%23%3e%3c_skipping_index")
              .isSpecialCharacter(true));

  @Test
  public void shouldVacuumIndexInDeletedState() {
    List<List<Object>> testCases =
        Lists.cartesianProduct(
            FLINT_TEST_DATASETS,
            List.of(DELETED),
            List.of(
                Pair.<EMRApiCall, EMRApiCall>of(
                    DEFAULT_OP,
                    () -> new GetJobRunResult().withJobRun(new JobRun().withState("Cancelled")))));

    runVacuumTestSuite(
        testCases,
        (mockDS, response) -> {
          assertEquals("SUCCESS", response.getStatus());
          assertFalse(flintIndexExists(mockDS.indexName));
          assertFalse(indexDocExists(mockDS.latestId));
        });
  }

  @Test
  public void shouldNotVacuumIndexInOtherStates() {
    List<List<Object>> testCases =
        Lists.cartesianProduct(
            FLINT_TEST_DATASETS,
            List.of(EMPTY, CREATING, ACTIVE, REFRESHING, VACUUMING),
            List.of(
                Pair.<EMRApiCall, EMRApiCall>of(
                    () -> {
                      throw new AssertionError("should not call cancelJobRun");
                    },
                    () -> {
                      throw new AssertionError("should not call getJobRunResult");
                    })));

    runVacuumTestSuite(
        testCases,
        (mockDS, response) -> {
          assertEquals("FAILED", response.getStatus());
          assertTrue(flintIndexExists(mockDS.indexName));
          assertTrue(indexDocExists(mockDS.latestId));
        });
  }

  private void runVacuumTestSuite(
      List<List<Object>> testCases,
      BiConsumer<FlintDatasetMock, AsyncQueryExecutionResponse> assertion) {
    testCases.forEach(
        params -> {
          FlintDatasetMock mockDS = (FlintDatasetMock) params.get(0);
          try {
            FlintIndexState state = (FlintIndexState) params.get(1);
            EMRApiCall cancelJobRun = ((Pair<EMRApiCall, EMRApiCall>) params.get(2)).getLeft();
            EMRApiCall getJobRunResult = ((Pair<EMRApiCall, EMRApiCall>) params.get(2)).getRight();

            AsyncQueryExecutionResponse response =
                runVacuumTest(mockDS, state, cancelJobRun, getJobRunResult);
            assertion.accept(mockDS, response);
          } finally {
            // Clean up because we simulate parameterized test in single unit test method
            if (flintIndexExists(mockDS.indexName)) {
              mockDS.deleteIndex();
            }
            if (indexDocExists(mockDS.latestId)) {
              deleteIndexDoc(mockDS.latestId);
            }
          }
        });
  }

  private AsyncQueryExecutionResponse runVacuumTest(
      FlintDatasetMock mockDS,
      FlintIndexState state,
      EMRApiCall<CancelJobRunResult> cancelJobRun,
      EMRApiCall<GetJobRunResult> getJobRunResult) {
    LocalEMRSClient emrsClient =
        new LocalEMRSClient() {
          @Override
          public CancelJobRunResult cancelJobRun(
              String applicationId, String jobId, boolean allowExceptionPropagation) {
            if (cancelJobRun == DEFAULT_OP) {
              return super.cancelJobRun(applicationId, jobId, allowExceptionPropagation);
            }
            return cancelJobRun.call();
          }

          @Override
          public GetJobRunResult getJobRunResult(String applicationId, String jobId) {
            if (getJobRunResult == DEFAULT_OP) {
              return super.getJobRunResult(applicationId, jobId);
            }
            return getJobRunResult.call();
          }
        };
    EMRServerlessClientFactory emrServerlessClientFactory = () -> emrsClient;
    AsyncQueryExecutorService asyncQueryExecutorService =
        createAsyncQueryExecutorService(emrServerlessClientFactory);

    // Mock Flint index
    mockDS.createIndex();

    // Mock index state doc
    MockFlintSparkJob flintIndexJob =
        new MockFlintSparkJob(flintIndexStateModelService, mockDS.latestId, "mys3");
    flintIndexJob.transition(state);

    // Vacuum index
    CreateAsyncQueryResponse response =
        asyncQueryExecutorService.createAsyncQuery(
            new CreateAsyncQueryRequest(mockDS.query, MYS3_DATASOURCE, LangType.SQL, null));

    return asyncQueryExecutorService.getAsyncQueryResults(response.getQueryId());
  }

  private boolean flintIndexExists(String flintIndexName) {
    return client
        .admin()
        .indices()
        .exists(new IndicesExistsRequest(flintIndexName))
        .actionGet()
        .isExists();
  }

  private boolean indexDocExists(String docId) {
    return client
        .get(new GetRequest(DATASOURCE_TO_REQUEST_INDEX.apply("mys3"), docId))
        .actionGet()
        .isExists();
  }

  private void deleteIndexDoc(String docId) {
    client.delete(new DeleteRequest(DATASOURCE_TO_REQUEST_INDEX.apply("mys3"), docId)).actionGet();
  }

  private FlintDatasetMock mockDataset(String query, FlintIndexType indexType, String indexName) {
    FlintDatasetMock dataset = new FlintDatasetMock(query, "", indexType, indexName);
    dataset.latestId(Base64.getEncoder().encodeToString(indexName.getBytes()));
    return dataset;
  }

  /**
   * EMR API call mock interface.
   *
   * @param <V> API call response type
   */
  @FunctionalInterface
  public interface EMRApiCall<V> {
    V call();
  }
}
