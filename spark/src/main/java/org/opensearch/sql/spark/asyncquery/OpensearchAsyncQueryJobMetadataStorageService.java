/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.execution.statestore.OpenSearchStateStoreUtil;
import org.opensearch.sql.spark.execution.statestore.StateStore;
import org.opensearch.sql.spark.execution.xcontent.AsyncQueryJobMetadataXContentSerializer;

/** Opensearch implementation of {@link AsyncQueryJobMetadataStorageService} */
@RequiredArgsConstructor
public class OpensearchAsyncQueryJobMetadataStorageService
    implements AsyncQueryJobMetadataStorageService {

  private final StateStore stateStore;
  private final AsyncQueryJobMetadataXContentSerializer asyncQueryJobMetadataXContentSerializer;

  private static final Logger LOGGER =
      LogManager.getLogger(OpensearchAsyncQueryJobMetadataStorageService.class);

  @Override
  public void storeJobMetadata(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    stateStore.create(
        asyncQueryJobMetadata,
        AsyncQueryJobMetadata::copy,
        OpenSearchStateStoreUtil.getIndexName(asyncQueryJobMetadata.getDatasourceName()));
  }

  @Override
  public Optional<AsyncQueryJobMetadata> getJobMetadata(String qid) {
    try {
      AsyncQueryId queryId = new AsyncQueryId(qid);
      return stateStore.get(
          queryId.docId(),
          asyncQueryJobMetadataXContentSerializer::fromXContent,
          OpenSearchStateStoreUtil.getIndexName(queryId.getDataSourceName()));
    } catch (Exception e) {
      LOGGER.error("Error while fetching the job metadata.", e);
      throw new AsyncQueryNotFoundException(String.format("Invalid QueryId: %s", qid));
    }
  }
}
