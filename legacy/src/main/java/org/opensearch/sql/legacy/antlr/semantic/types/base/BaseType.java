/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.types.base;

import java.util.List;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;

/** Base type interface */
public interface BaseType extends Type {

  @Override
  default Type construct(List<Type> others) {
    return this;
  }

  @Override
  default String usage() {
    return getName();
  }
}
