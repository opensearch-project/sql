/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.sql.opensearch.planner.physical;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.immutables.value.Value;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteOpenSearchIndexScan;

/** Planner rule that push a {@link Project} down to {@link CalciteOpenSearchIndexScan} */
@Value.Enclosing
public class OpenSearchFilterIndexScanRule extends RelRule<OpenSearchFilterIndexScanRule.Config> {

  /** Creates a OpenSearchFilterIndexScanRule. */
  protected OpenSearchFilterIndexScanRule(Config config) {
    super(config);
  }

  // ~ Methods ----------------------------------------------------------------

  protected static boolean test(CalciteOpenSearchIndexScan scan) {
    final RelOptTable table = scan.getTable();
    return table.unwrap(OpenSearchIndex.class) != null;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (call.rels.length == 2) {
      // the ordinary variant
      final Filter filter = call.rel(0);
      final CalciteOpenSearchIndexScan scan = call.rel(1);
      apply(call, filter, scan);
    } else {
      throw new AssertionError();
    }
  }

  protected void apply(RelOptRuleCall call, Filter filter, CalciteOpenSearchIndexScan scan) {
    if (scan.pushDownFilter(filter)) {
      call.transformTo(scan);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    /** Config that matches Project on CalciteOpenSearchIndexScan. */
    Config DEFAULT =
        ImmutableOpenSearchFilterIndexScanRule.Config.builder()
            .build()
            .withOperandSupplier(
                b0 ->
                    b0.operand(Filter.class)
                        .oneInput(
                            b1 ->
                                b1.operand(CalciteOpenSearchIndexScan.class)
                                    .predicate(OpenSearchFilterIndexScanRule::test)
                                    .noInputs()));

    @Override
    default OpenSearchFilterIndexScanRule toRule() {
      return new OpenSearchFilterIndexScanRule(this);
    }
  }
}
