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
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.test.OpenSearchIntegTestCase;

public class OpensearchAsyncQueryAsyncQueryJobMetadataStorageServiceTest
    extends OpenSearchIntegTestCase {

  public static final String DS_NAME = "mys3";
  private static final String MOCK_SESSION_ID = "sessionId";
  private static final String MOCK_RESULT_INDEX = "resultIndex";
  private static final String MOCK_QUERY_ID = "00fdo6u94n7abo0q";
  private OpensearchAsyncQueryJobMetadataStorageService opensearchJobMetadataStorageService;

  @Before
  public void setup() {
    opensearchJobMetadataStorageService =
        new OpensearchAsyncQueryJobMetadataStorageService(
            new StateStore(client(), clusterService()));
  }

  @Test
  public void testStoreJobMetadata() {
    AsyncQueryJobMetadata expected =
        new AsyncQueryJobMetadata(
            AsyncQueryId.newAsyncQueryId(DS_NAME),
            EMR_JOB_ID,
            EMRS_APPLICATION_ID,
            MOCK_RESULT_INDEX);

    opensearchJobMetadataStorageService.storeJobMetadata(expected);
    Optional<AsyncQueryJobMetadata> actual =
        opensearchJobMetadataStorageService.getJobMetadata(expected.getQueryId().getId());

    assertTrue(actual.isPresent());
    assertEquals(expected, actual.get());
    assertEquals(expected, actual.get());
    assertNull(actual.get().getSessionId());
  }

  @Test
  public void testStoreJobMetadataWithResultExtraData() {
    AsyncQueryJobMetadata expected =
        new AsyncQueryJobMetadata(
            AsyncQueryId.newAsyncQueryId(DS_NAME),
            EMR_JOB_ID,
            EMRS_APPLICATION_ID,
            MOCK_RESULT_INDEX,
            MOCK_SESSION_ID);

    opensearchJobMetadataStorageService.storeJobMetadata(expected);
    Optional<AsyncQueryJobMetadata> actual =
        opensearchJobMetadataStorageService.getJobMetadata(expected.getQueryId().getId());

    assertTrue(actual.isPresent());
    assertEquals(expected, actual.get());
    assertEquals("resultIndex", actual.get().getResultLocation());
    assertEquals(MOCK_SESSION_ID, actual.get().getSessionId());
  }

  @Test
  public void testGetJobMetadataWithMalformedQueryId() {
    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> opensearchJobMetadataStorageService.getJobMetadata(MOCK_QUERY_ID));
    Assertions.assertEquals(
        String.format("Invalid QueryId: %s", MOCK_QUERY_ID),
        asyncQueryNotFoundException.getMessage());
  }

  @Test
  public void testGetJobMetadataWithEmptyQueryId() {
    AsyncQueryNotFoundException asyncQueryNotFoundException =
        Assertions.assertThrows(
            AsyncQueryNotFoundException.class,
            () -> opensearchJobMetadataStorageService.getJobMetadata(""));
    Assertions.assertEquals("Invalid QueryId: ", asyncQueryNotFoundException.getMessage());
  }
}
