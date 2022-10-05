/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.stream;

import java.util.Optional;

public interface StreamSource {

  /**
   * Get current {@link Offset} of stream.
   * @return empty if the stream does not have new events.
   */
  Optional<Offset> getLatestOffset();

  /**
   * Get {@link Batch} from source between (start, end].
   * @param start start offset.
   * @param end end offset.
   * @return {@link Batch}.
   */
  Batch getBatch(Optional<Offset> start, Offset end);

  // todo, need?
  void commit(Offset offset);
}
