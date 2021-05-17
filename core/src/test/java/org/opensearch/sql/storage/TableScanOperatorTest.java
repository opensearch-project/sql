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
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

class TableScanOperatorTest {

  private final TableScanOperator tableScan = new TableScanOperator() {
    @Override
    public String explain() {
      return "explain";
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public ExprValue next() {
      return null;
    }
  };

  @Test
  public void accept() {
    Boolean isVisited = tableScan.accept(new PhysicalPlanNodeVisitor<Boolean, Object>() {
      @Override
      protected Boolean visitNode(PhysicalPlan node, Object context) {
        return (node instanceof TableScanOperator);
      }

      @Override
      public Boolean visitTableScan(TableScanOperator node, Object context) {
        return super.visitTableScan(node, context);
      }
    }, null);

    assertTrue(isVisited);
  }

  @Test
  public void getChild() {
    assertTrue(tableScan.getChild().isEmpty());
  }

}
