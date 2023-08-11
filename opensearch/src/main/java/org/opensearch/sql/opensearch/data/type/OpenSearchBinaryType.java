/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import lombok.EqualsAndHashCode;

/**
 * The type of a binary value. See <a
 * href="https://opensearch.org/docs/latest/opensearch/supported-field-types/binary/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchBinaryType extends OpenSearchDataType {

  private static final OpenSearchBinaryType instance = new OpenSearchBinaryType();

  private OpenSearchBinaryType() {
    super(MappingType.Binary);
    exprCoreType = UNKNOWN;
  }

  public static OpenSearchBinaryType of() {
    return OpenSearchBinaryType.instance;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return instance;
  }
}
