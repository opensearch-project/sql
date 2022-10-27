/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.catalog.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.storage.StorageEngine;

@Getter
@RequiredArgsConstructor
@EqualsAndHashCode
public class Catalog {

  private final String name;

  private final ConnectorType connectorType;

  @EqualsAndHashCode.Exclude
  private final StorageEngine storageEngine;

}
