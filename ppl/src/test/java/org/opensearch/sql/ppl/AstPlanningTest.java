/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.mockito.Mockito;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;

/** Base class for tests for the AST query planner. */
public class AstPlanningTest {
  protected final Settings settings = Mockito.mock(Settings.class);
  protected final PPLSyntaxParser parser = new PPLSyntaxParser();

  protected Node plan(String query) {
    AstBuilder astBuilder = new AstBuilder(query, settings);
    return astBuilder.visit(parser.parse(query));
  }
}
