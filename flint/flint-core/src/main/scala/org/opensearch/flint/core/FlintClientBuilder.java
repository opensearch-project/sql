/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core;

import org.opensearch.flint.core.storage.FlintOpenSearchClient;

/**
 * {@link FlintClient} builder.
 */
public class FlintClientBuilder {

  public static FlintClient build(FlintOptions options) {
    return new FlintOpenSearchClient(options);
  }
}
