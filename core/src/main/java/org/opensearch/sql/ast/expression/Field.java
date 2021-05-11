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
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.ast.expression;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Field extends UnresolvedExpression {

  private final UnresolvedExpression field;

  private final List<Argument> fieldArgs;

  /**
   * Constructor of Field.
   */
  public Field(UnresolvedExpression field) {
    this(field, Collections.emptyList());
  }

  /**
   * Constructor of Field.
   */
  public Field(UnresolvedExpression field, List<Argument> fieldArgs) {
    this.field = field;
    this.fieldArgs = fieldArgs;
  }

  public boolean hasArgument() {
    return !fieldArgs.isEmpty();
  }

  @Override
  public List<UnresolvedExpression> getChild() {
    return ImmutableList.of(this.field);
  }

  @Override
  public <R, C> R accept(AbstractNodeVisitor<R, C> nodeVisitor, C context) {
    return nodeVisitor.visitField(this, context);
  }
}
