/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.streaming;

import java.util.Optional;

/**
 * Streaming source.
 */
public interface StreamingSource {
  /**
   * Get current {@link Offset} of stream data.
   *
   * @return empty if the stream does not has new data.
   */
  Optional<Offset> getLatestOffset();

  /**
   * Get a {@link Batch} from source between (start, end].
   *
   * @param start start offset.
   * @param end end offset.
   * @return @link Batch}.
   */
  Batch getBatch(Optional<Offset> start, Offset end);
}
