package org.opensearch.sql.spark.flint;

import org.opensearch.sql.spark.dispatcher.model.IndexQueryDetails;

/** Interface for FlintIndexMetadataReader */
public interface FlintIndexMetadataReader {

  /**
   * Given Index details, get the streaming job Id.
   *
   * @param indexQueryDetails indexDetails.
   * @return FlintIndexMetadata.
   */
  FlintIndexMetadata getFlintIndexMetadata(IndexQueryDetails indexQueryDetails);
}
