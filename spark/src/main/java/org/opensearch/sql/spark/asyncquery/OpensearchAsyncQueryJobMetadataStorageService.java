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
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryId;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.execution.statestore.StateStore;

/** Opensearch implementation of {@link AsyncQueryJobMetadataStorageService} */
@RequiredArgsConstructor
public class OpensearchAsyncQueryJobMetadataStorageService
    implements AsyncQueryJobMetadataStorageService {

  private final StateStore stateStore;

  @Override
  public void storeJobMetadata(AsyncQueryJobMetadata asyncQueryJobMetadata) {
    AsyncQueryId queryId = asyncQueryJobMetadata.getQueryId();
    createJobMetaData(stateStore, queryId.getDataSourceName()).apply(asyncQueryJobMetadata);
  }

  @Override
  public Optional<AsyncQueryJobMetadata> getJobMetadata(String qid) {
    AsyncQueryId queryId = new AsyncQueryId(qid);
    return StateStore.getJobMetaData(stateStore, queryId.getDataSourceName())
        .apply(queryId.docId());
  }
}
