/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.storage;

import java.util.function.Function;

public class CatalogService {
  private final Function<String, StorageEngine> mapping;

  public CatalogService(
      Function<String, StorageEngine> mapping) {
    this.mapping = mapping;
  }

  public StorageEngine getStorageEngine(String tableName) {
    return mapping.apply(tableName);
  }
}
