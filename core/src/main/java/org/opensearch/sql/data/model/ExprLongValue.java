/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.data.model;

import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Expression Long Value.
 */
public class ExprLongValue extends AbstractExprNumberValue {

  public ExprLongValue(Number value) {
    super(value);
  }

  @Override
  public Object value() {
    return longValue();
  }

  @Override
  public ExprType type() {
    return ExprCoreType.LONG;
  }

  @Override
  public String toString() {
    return longValue().toString();
  }

  @Override
  public int compare(ExprValue other) {
    return Long.compare(longValue(), other.longValue());
  }

  @Override
  public boolean equal(ExprValue other) {
    return longValue().equals(other.longValue());
  }
}
