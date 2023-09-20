/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.asyncquery;

import java.util.Optional;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryJobMetadata;

public interface AsyncQueryJobMetadataStorageService {

  void storeJobMetadata(AsyncQueryJobMetadata asyncQueryJobMetadata);

  Optional<AsyncQueryJobMetadata> getJobMetadata(String jobId);
}
