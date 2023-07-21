/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasource.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.storage.StorageEngine;

/**
 * Each user configured datasource mapping to one instance of DataSource per JVM.
 */
@Getter
@RequiredArgsConstructor
@EqualsAndHashCode
public class DataSource {

  private final String name;

  private final DataSourceType connectorType;

  @EqualsAndHashCode.Exclude
  private final StorageEngine storageEngine;

}
