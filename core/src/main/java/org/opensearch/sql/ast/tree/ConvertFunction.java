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

/**
 * Represents a single conversion function within a convert command.
 *
 * <p>Example: auto(field1, field2) AS converted_field
 */
@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class ConvertFunction {
  /** The name of the conversion function (e.g., "auto", "num", "ctime"). */
  private final String functionName;

  /** The list of field names or patterns to convert. */
  private final List<String> fieldList;

  /** Optional alias for the converted field (AS clause). Null if not specified. */
  private final String asField;
}
