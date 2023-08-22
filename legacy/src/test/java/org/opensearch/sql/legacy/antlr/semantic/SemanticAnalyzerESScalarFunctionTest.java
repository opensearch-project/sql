/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic;

import org.junit.Test;

/** Semantic analysis test for Elaticsearch special scalar functions */
public class SemanticAnalyzerESScalarFunctionTest extends SemanticAnalyzerTestBase {

  @Test
  public void dateFunctionCallWithDateInSelectClauseShouldPass() {
    validate("SELECT DAY_OF_MONTH(birthday) FROM semantics");
    validate("SELECT DAY_OF_WEEK(birthday) FROM semantics");
    validate("SELECT DAY_OF_YEAR(birthday) FROM semantics");
    validate("SELECT MINUTE_OF_DAY(birthday) FROM semantics");
    validate("SELECT MINUTE_OF_HOUR(birthday) FROM semantics");
    validate("SELECT MONTH_OF_YEAR(birthday) FROM semantics");
    validate("SELECT WEEK_OF_YEAR(birthday) FROM semantics");
  }

  @Test
  public void dateFunctionCallWithDateInWhereClauseShouldPass() {
    validate("SELECT * FROM semantics WHERE DAY_OF_MONTH(birthday) = 1");
    validate("SELECT * FROM semantics WHERE DAY_OF_WEEK(birthday) = 1");
    validate("SELECT * FROM semantics WHERE DAY_OF_YEAR(birthday) = 1");
    validate("SELECT * FROM semantics WHERE MINUTE_OF_DAY(birthday) = 1");
    validate("SELECT * FROM semantics WHERE MINUTE_OF_HOUR(birthday) = 1");
    validate("SELECT * FROM semantics WHERE MONTH_OF_YEAR(birthday) = 1");
    validate("SELECT * FROM semantics WHERE WEEK_OF_YEAR(birthday) = 1");
  }

  @Test
  public void geoFunctionCallWithGeoPointInWhereClauseShouldPass() {
    validate("SELECT * FROM semantics WHERE GEO_BOUNDING_BOX(location, 100.0, 1.0, 101, 0.0)");
    validate("SELECT * FROM semantics WHERE GEO_DISTANCE(location, '1km', 100.5, 0.500001)");
    validate("SELECT * FROM semantics WHERE GEO_DISTANCE_RANGE(location, '1km', 100.5, 0.500001)");
  }

  @Test
  public void fullTextMatchFunctionCallWithStringInWhereClauseShouldPass() {
    validate("SELECT * FROM semantics WHERE MATCH_PHRASE(address, 'Seattle')");
    validate("SELECT * FROM semantics WHERE MATCHPHRASE(employer, 'Seattle')");
    validate("SELECT * FROM semantics WHERE MATCH_QUERY(manager.name, 'Seattle')");
    validate("SELECT * FROM semantics WHERE MATCHQUERY(manager.name, 'Seattle')");
    validate("SELECT * FROM semantics WHERE QUERY('Seattle')");
    validate("SELECT * FROM semantics WHERE WILDCARD_QUERY(manager.name, 'Sea*')");
    validate("SELECT * FROM semantics WHERE WILDCARDQUERY(manager.name, 'Sea*')");
  }
}
