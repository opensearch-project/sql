/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.storage;

import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/**
 * S3 Storage Engine.
 */
public class S3StorageEngine implements StorageEngine {

  @Override
  public Table getTable(String name) {
    return new S3Table(name);
  }
}
