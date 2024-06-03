/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_JOB_ID;

import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.execution.xcontent.AsyncQueryJobMetadataXContentSerializer;
import org.opensearch.sql.spark.utils.IDUtils;
import org.opensearch.test.OpenSearchIntegTestCase;

public class OpenSearchAsyncQueryJobMetadataStorageServiceTest
    extends OpenSearchIntegTestCase {

  public static final String DS_NAME = "mys3";
  private static final String MOCK_SESSION_ID = "sessionId";
  private static final String MOCK_RESULT_INDEX = "resultIndex";
  private static final String MOCK_QUERY_ID = "00fdo6u94n7abo0q";
  private OpenSearchAsyncQueryJobMetadataStorageService openSearchJobMetadataStorageService;

  @Before
  public void setup() {
    openSearchJobMetadataStorageService =
        new OpenSearchAsyncQueryJobMetadataStorageService(
            new StateStore(client(), clusterService()),
            new AsyncQueryJobMetadataXContentSerializer());
  }

  @Test
  public void testStoreJobMetadata() {
    AsyncQueryJobMetadata expected =
        AsyncQueryJobMetadata.builder()
            .queryId(IDUtils.encode(DS_NAME))
            .jobId(EMR_JOB_ID)
            .applicationId(EMRS_APPLICATION_ID)
            .resultIndex(MOCK_RESULT_INDEX)
            .datasourceName(DS_NAME)
            .build();

    openSearchJobMetadataStorageService.storeJobMetadata(expected);
    Optional<AsyncQueryJobMetadata> actual =
        openSearchJobMetadataStorageService.getJobMetadata(expected.getQueryId());

    assertTrue(actual.isPresent());
    assertEquals(expected, actual.get());
    assertEquals(expected, actual.get());
    assertNull(actual.get().getSessionId());
  }

  @Test
  public void testStoreJobMetadataWithResultExtraData() {
    AsyncQueryJobMetadata expected =
        AsyncQueryJobMetadata.builder()
            .queryId(IDUtils.encode(DS_NAME))
            .jobId(EMR_JOB_ID)
            .applicationId(EMRS_APPLICATION_ID)
            .resultIndex(MOCK_RESULT_INDEX)
            .sessionId(MOCK_SESSION_ID)
            .datasourceName(DS_NAME)
            .build();

    openSearchJobMetadataStorageService.storeJobMetadata(expected);
    Optional<AsyncQueryJobMetadata> actual =
        openSearchJobMetadataStorageService.getJobMetadata(expected.getQueryId());

    assertTrue(actual.isPresent());
    assertEquals(expected, actual.get());
    assertEquals(MOCK_RESULT_INDEX, actual.get().getResultIndex());
    assertEquals(MOCK_SESSION_ID, actual.get().getSessionId());
  }

  @Test
  public void testGetJobMetadataWithMalformedQueryId() {
    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> openSearchJobMetadataStorageService.getJobMetadata(MOCK_QUERY_ID));
    Assertions.assertEquals(
        String.format("Invalid QueryId: %s", MOCK_QUERY_ID),
        asyncQueryNotFoundException.getMessage());
  }

  @Test
  public void testGetJobMetadataWithEmptyQueryId() {
    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> openSearchJobMetadataStorageService.getJobMetadata(""));
    Assertions.assertEquals("Invalid QueryId: ", asyncQueryNotFoundException.getMessage());
  }
}
