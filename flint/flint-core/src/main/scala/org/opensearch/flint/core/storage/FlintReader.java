/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.storage;

/**
 * Flint Reader Interface
 */
public interface FlintReader {

  /**
   * true if next doc exist.
   */
  boolean hasNext();

  /**
   * Return next doc in String.
   */
  String next();

  /**
   * close.
   */
  void close();
}
