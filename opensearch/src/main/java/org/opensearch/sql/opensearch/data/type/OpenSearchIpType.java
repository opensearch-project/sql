/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import lombok.EqualsAndHashCode;

/**
 * The type of an ip value. See <a
 * href="https://opensearch.org/docs/latest/opensearch/supported-field-types/ip/">doc</a>
 */
@EqualsAndHashCode(callSuper = false)
public class OpenSearchIpType extends OpenSearchDataType {

  private static final OpenSearchIpType instance = new OpenSearchIpType();

  private OpenSearchIpType() {
    super(MappingType.Ip);
    exprCoreType = STRING;
  }

  public static OpenSearchIpType of() {
    return OpenSearchIpType.instance;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    return instance;
  }
}
