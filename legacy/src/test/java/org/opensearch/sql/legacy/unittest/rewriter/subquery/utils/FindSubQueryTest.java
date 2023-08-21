/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.rewriter.subquery.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.util.SqlParserUtils.parse;

import org.junit.Test;
import org.opensearch.sql.legacy.rewriter.subquery.utils.FindSubQuery;

public class FindSubQueryTest {

  @Test
  public void hasInSubQuery() {
    FindSubQuery findSubQuery = new FindSubQuery();

    parse("SELECT * FROM TbA " + "WHERE a in (SELECT b FROM TbB)").accept(findSubQuery);
    assertTrue(findSubQuery.hasSubQuery());
    assertFalse(findSubQuery.getSqlInSubQueryExprs().isEmpty());
    assertEquals(1, findSubQuery.getSqlInSubQueryExprs().size());
  }

  @Test
  public void hasExistSubQuery() {
    FindSubQuery findSubQuery = new FindSubQuery();

    parse("SELECT * FROM TbA WHERE EXISTS (SELECT * FROM TbB)").accept(findSubQuery);
    assertTrue(findSubQuery.hasSubQuery());
    assertFalse(findSubQuery.getSqlExistsExprs().isEmpty());
    assertEquals(1, findSubQuery.getSqlExistsExprs().size());
  }

  @Test
  public void stopVisitWhenFound() {
    FindSubQuery findSubQuery = new FindSubQuery().continueVisitWhenFound(false);

    parse("SELECT * FROM TbA WHERE a in (SELECT b FROM TbB WHERE b2 in (SELECT c FROM Tbc))")
        .accept(findSubQuery);
    assertTrue(findSubQuery.hasSubQuery());
    assertFalse(findSubQuery.getSqlInSubQueryExprs().isEmpty());
    assertEquals(1, findSubQuery.getSqlInSubQueryExprs().size());
  }
}
