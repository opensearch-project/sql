/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr;

import org.opensearch.sql.common.antlr.SyntaxParserTestBase;

public class SQLParserTest extends SyntaxParserTestBase {
  public SQLParserTest() {
    super(new SQLSyntaxParser());
  }
}
