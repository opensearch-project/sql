/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.response;

/**
 * Response listener for response post-processing callback. This is necessary because execution
 * engine may schedule and execute in different thread.
 *
 * @param <R> response class
 */
public interface ResponseListener<R> {

  /**
   * Handle successful response.
   *
   * @param response successful response
   */
  void onResponse(R response);

  /**
   * Handle failed response.
   *
   * @param e exception captured
   */
  void onFailure(Exception e);
}
