/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

/** Factory interface for creating instances of {@link EMRServerlessClient}. */
public interface EMRServerlessClientFactory {

  /**
   * Gets an instance of {@link EMRServerlessClient}.
   *
   * @return An {@link EMRServerlessClient} instance.
   */
  EMRServerlessClient getClient();
}
