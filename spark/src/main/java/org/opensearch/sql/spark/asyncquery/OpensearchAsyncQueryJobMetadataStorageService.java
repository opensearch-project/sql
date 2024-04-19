/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery;

import static org.opensearch.sql.spark.execution.statestore.StateStore.createJobMetaData;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.execution.statestore.StateStore;

/** Opensearch implementation of {@link AsyncQueryJobMetadataStorageService} */
@RequiredArgsConstructor
public class OpensearchAsyncQueryJobMetadataStorageService
    implements AsyncQueryJobMetadataStorageService {

  private final StateStore stateStore;

  private static final Logger LOGGER =
      LogManager.getLogger(OpensearchAsyncQueryJobMetadataStorageService.class);

  @Override
  public void storeJobMetadata(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    AsyncQueryId queryId = asyncQueryJobMetadata.getQueryId();
    createJobMetaData(stateStore, queryId.getDataSourceName()).apply(asyncQueryJobMetadata);
  }

  @Override
  public Optional<AsyncQueryJobMetadata> getJobMetadata(String qid) {
    try {
      AsyncQueryId queryId = new AsyncQueryId(qid);
      return StateStore.getJobMetaData(stateStore, queryId.getDataSourceName())
          .apply(queryId.docId());
    } catch (Exception e) {
      LOGGER.error("Error while fetching the job metadata.", e);
      throw new AsyncQueryNotFoundException(String.format("Invalid QueryId: %s", qid));
    }
  }
}
