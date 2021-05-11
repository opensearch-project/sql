/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.data.type;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import java.util.Arrays;
import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;

/**
 * The Type of {@link Expression} and {@link ExprValue}.
 */
public interface ExprType {
  /**
   * Is compatible with other types.
   */
  default boolean isCompatible(ExprType other) {
    if (this.equals(other)) {
      return true;
    } else {
      if (other.equals(UNKNOWN)) {
        return false;
      }
      for (ExprType parentTypeOfOther : other.getParent()) {
        if (isCompatible(parentTypeOfOther)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Get the parent type.
   */
  default List<ExprType> getParent() {
    return Arrays.asList(UNKNOWN);
  }

  /**
   * Get the type name.
   */
  String typeName();

  /**
   * Get the legacy type name for old engine.
   */
  default String legacyTypeName() {
    return typeName();
  }
}
