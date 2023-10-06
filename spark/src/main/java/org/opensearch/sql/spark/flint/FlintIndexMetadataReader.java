package org.opensearch.sql.spark.flint;

import org.opensearch.sql.spark.dispatcher.model.IndexDetails;

/** Interface for FlintIndexMetadataReader */
public interface FlintIndexMetadataReader {

  /**
   * Given Index details, get the streaming job Id.
   *
   * @param indexDetails indexDetails.
   * @return jobId.
   */
  FlintIndexMetadata getJobIdFromFlintIndexMetadata(IndexDetails indexDetails);
}
