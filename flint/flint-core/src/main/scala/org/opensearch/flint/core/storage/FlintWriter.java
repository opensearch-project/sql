/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

import java.io.Writer;

/**
 * Extend {@link Writer}, not specific method defined for now.
 */
public abstract class FlintWriter extends Writer {

  /**
   * Creates a document if it doesn’t already exist and returns an error otherwise. The next line must include a JSON document.
   *
   * { "create": { "_index": "movies", "_id": "tt1392214" } }
   * { "title": "Prisoners", "year": 2013 }
   */
  public static final String ACTION_CREATE = "create";
}
