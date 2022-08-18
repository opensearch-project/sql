/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl.table;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Table definition.
 */
@EqualsAndHashCode
@Getter
@RequiredArgsConstructor
@ToString
public class Schema {

  private final String tableName;

}
