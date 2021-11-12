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

import java.util.Objects;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Expression Null Value.
 */
public class ExprNullValue extends AbstractExprValue {
  private static final ExprNullValue instance = new ExprNullValue();

  private ExprNullValue() {
  }

  @Override
  public int hashCode() {
    return Objects.hashCode("NULL");
  }

  @Override
  public String toString() {
    return "NULL";
  }

  public static ExprNullValue of() {
    return instance;
  }

  @Override
  public Object value() {
    return null;
  }

  @Override
  public ExprType type() {
    return ExprCoreType.UNDEFINED;
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public int compare(ExprValue other) {
    throw new IllegalStateException(
        String.format("[BUG] Unreachable, Comparing with NULL is undefined"));
  }

  /**
   * NULL value is equal to NULL value.
   * Notes, this function should only used for Java Object Compare.
   */
  @Override
  public boolean equal(ExprValue other) {
    return other.isNull();
  }
}
