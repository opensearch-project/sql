/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/** Represents a single conversion function within a convert command. */
@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class ConvertFunction {
  private final String functionName;
  private final List<String> fieldList;
  private final String asField;
}
