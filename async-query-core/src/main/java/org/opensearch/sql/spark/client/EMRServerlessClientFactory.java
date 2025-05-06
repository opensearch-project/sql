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
   * @param accountId Account ID of the requester. It will be used to decide the cluster.
   * @return An {@link EMRServerlessClient} instance.
   */
  EMRServerlessClient getClient(String accountId);
}
