/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.execution.xcontent.AsyncQueryJobMetadataXContentSerializer;
import org.opensearch.sql.spark.utils.IDUtils;

/** OpenSearch implementation of {@link AsyncQueryJobMetadataStorageService} */
@RequiredArgsConstructor
public class OpenSearchAsyncQueryJobMetadataStorageService
    implements AsyncQueryJobMetadataStorageService {

  private final StateStore stateStore;
  private final AsyncQueryJobMetadataXContentSerializer asyncQueryJobMetadataXContentSerializer;

  private static final Logger LOGGER =
      LogManager.getLogger(OpenSearchAsyncQueryJobMetadataStorageService.class);

  @Override
  public void storeJobMetadata(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext) {
    stateStore.create(
        mapIdToDocumentId(asyncQueryJobMetadata.getId()),
        asyncQueryJobMetadata,
        AsyncQueryJobMetadata::copy,
        OpenSearchStateStoreUtil.getIndexName(asyncQueryJobMetadata.getDatasourceName()));
  }

  private String mapIdToDocumentId(String id) {
    return "qid" + id;
  }

  @Override
  public Optional<AsyncQueryJobMetadata> getJobMetadata(String queryId) {
    try {
      return stateStore.get(
          mapIdToDocumentId(queryId),
          asyncQueryJobMetadataXContentSerializer::fromXContent,
          OpenSearchStateStoreUtil.getIndexName(IDUtils.decode(queryId)));
    } catch (Exception e) {
      LOGGER.error("Error while fetching the job metadata.", e);
      throw new AsyncQueryNotFoundException(String.format("Invalid QueryId: %s", queryId));
    }
  }
}
