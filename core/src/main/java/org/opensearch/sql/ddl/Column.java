/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ddl;

import lombok.Data;

@Data
public class Column {

  private final String name;
  private final String type; // TODO: unresolved expression -> expr type ?

}
