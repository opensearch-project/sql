/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.storage;

import javax.xml.transform.stream.StreamSource;

/**
 * A data source could represent as streaming source.
 */
public interface StreamTable {

  default StreamSource toStream() {
    throw new UnsupportedOperationException("to stream is not supported.");
  }
}
