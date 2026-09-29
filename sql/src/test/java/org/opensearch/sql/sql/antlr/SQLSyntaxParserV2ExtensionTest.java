/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.antlr;

import org.junit.jupiter.api.Test;

/** Tests for V2 grammar extensions (JOIN). */
class SQLSyntaxParserV2ExtensionTest extends SQLParserTest {

  @Test
  void canParseInnerJoin() {
    acceptQuery("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
  }

  @Test
  void canParseLeftJoin() {
    acceptQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id");
  }

  @Test
  void canParseRightJoin() {
    acceptQuery("SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id");
  }

  @Test
  void canParseCrossJoin() {
    acceptQuery("SELECT * FROM t1 CROSS JOIN t2");
  }

  @Test
  void canParseLeftOuterJoin() {
    acceptQuery("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id");
  }
}
