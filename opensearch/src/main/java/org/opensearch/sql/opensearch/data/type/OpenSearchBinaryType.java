/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * The type of a binary value. See
 * <a href="https://opensearch.org/docs/latest/opensearch/supported-field-types/binary/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchBinaryType extends OpenSearchDataType {

  @Getter
  private static final OpenSearchBinaryType instance = new OpenSearchBinaryType();

  private OpenSearchBinaryType() {
    super(MappingType.Binary);
    exprCoreType = UNKNOWN;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return instance;
  }
}
