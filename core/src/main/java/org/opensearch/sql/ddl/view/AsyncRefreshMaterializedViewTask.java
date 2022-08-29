/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.ddl.view;

import static org.opensearch.sql.ast.dsl.AstDSL.longLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;
import static org.opensearch.sql.ast.dsl.AstDSL.stringLiteral;
import static org.opensearch.sql.ast.dsl.AstDSL.values;
import static org.opensearch.sql.ast.dsl.AstDSL.write;

import java.util.Arrays;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.ddl.DataDefinitionTask;

@RequiredArgsConstructor
public class AsyncRefreshMaterializedViewTask extends DataDefinitionTask {

  private final UnresolvedPlan writePlan;

  private final String viewName;

  @Override
  public ExprValue execute() {
    try {
      // 1. execute write plan
      queryService.execute(writePlan);

      // 2. Refresh stateviews
      UnresolvedPlan insertStateView =
          write(
              values(
                  Arrays.asList(
                      stringLiteral(viewName),
                      stringLiteral("refresh view successfully"),
                      longLiteral(System.currentTimeMillis()))),
              qualifiedName(".stateviews"),
              Arrays.asList(
                  qualifiedName("viewName"),
                  qualifiedName("viewstatus"),
                  qualifiedName("timestamp")));
      queryService.execute(insertStateView);
      return ExprValueUtils.missingValue();
    } catch (Exception e) {
      // 2. Refresh stateviews
      UnresolvedPlan insertStateView =
          write(
              values(
                  Arrays.asList(
                      stringLiteral(viewName),
                      stringLiteral("refresh view failed"),
                      longLiteral(System.currentTimeMillis()))),
              qualifiedName(".stateviews"),
              Arrays.asList(
                  qualifiedName("viewName"),
                  qualifiedName("viewstatus"),
                  qualifiedName("timestamp")));
      queryService.execute(insertStateView);
      return ExprValueUtils.missingValue();
    }
  }
}
