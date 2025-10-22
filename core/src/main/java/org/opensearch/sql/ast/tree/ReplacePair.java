/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Literal;

/** A pair of pattern and replacement literals for the Replace command. */
@Getter
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class ReplacePair {
  private final Literal pattern;
  private final Literal replacement;
}
