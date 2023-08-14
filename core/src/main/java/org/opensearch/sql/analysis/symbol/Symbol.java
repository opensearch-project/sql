/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analysis.symbol;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/** Symbol in the scope. */
@ToString
@Getter
@RequiredArgsConstructor
public class Symbol {
  private final Namespace namespace;
  private final String name;
}
