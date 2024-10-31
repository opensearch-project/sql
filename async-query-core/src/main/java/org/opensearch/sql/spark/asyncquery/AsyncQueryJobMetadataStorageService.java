/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery;

import java.util.Optional;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryRequestContext;
import org.opensearch.sql.spark.asyncquery.model.QueryState;

public interface AsyncQueryJobMetadataStorageService {

  void storeJobMetadata(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      AsyncQueryRequestContext asyncQueryRequestContext);

  void updateState(
      AsyncQueryJobMetadata asyncQueryJobMetadata,
      QueryState newState,
      AsyncQueryRequestContext asyncQueryRequestContext);

  Optional<AsyncQueryJobMetadata> getJobMetadata(String jobId);
}
