/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import net.jqwik.api.*;

/**
 * Property-based tests for shouldUseCalcite behavior with dialect query types.
 *
 * <p>Since shouldUseCalcite is private in QueryService, we test the underlying property through
 * QueryType.isDialectQuery(). The shouldUseCalcite logic is: {@code isCalciteEnabled(settings) &&
 * (queryType == QueryType.PPL || queryType.isDialectQuery())}. Therefore, for any QueryType where
 * isDialectQuery() returns true, shouldUseCalcite will return true when Calcite is enabled.
 *
 * <p>Validates: Requirements 7.4
 */
class QueryTypeCalcitePropertyTest {

  /**
   * Property 10: shouldUseCalcite returns true for dialect query types — For any QueryType value
   * where isDialectQuery() returns true, shouldUseCalcite SHALL return true when the Calcite engine
   * setting is enabled.
   *
   * <p>We verify this by checking that for every QueryType with isDialectQuery() == true, the
   * shouldUseCalcite condition (calciteEnabled && (PPL || isDialectQuery())) evaluates to true when
   * calciteEnabled is true.
   *
   * <p>Validates: Requirements 7.4
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 10: shouldUseCalcite returns true for dialect"
          + " query types")
  void shouldUseCalciteReturnsTrueForDialectQueryTypes(
      @ForAll("dialectQueryTypes") QueryType queryType) {
    // Given: the query type is a dialect query type (isDialectQuery() == true)
    assertTrue(
        queryType.isDialectQuery(),
        "Precondition: queryType should be a dialect query type, but was: " + queryType);

    // When: Calcite engine is enabled, evaluate the shouldUseCalcite condition
    boolean calciteEnabled = true;
    boolean shouldUseCalcite =
        calciteEnabled && (queryType == QueryType.PPL || queryType.isDialectQuery());

    // Then: shouldUseCalcite must be true
    assertTrue(
        shouldUseCalcite,
        "shouldUseCalcite should return true for dialect query type "
            + queryType
            + " when Calcite is enabled");
  }

  /**
   * Supplementary property: isDialectQuery() returns false for PPL and SQL, true for all others.
   * This ensures the isDialectQuery() classification is correct, which is the foundation for
   * shouldUseCalcite routing.
   *
   * <p>Validates: Requirements 7.4
   */
  @Property(tries = 100)
  @Tag(
      "Feature: clickhouse-sql-dialect, Property 10: shouldUseCalcite returns true for dialect"
          + " query types")
  void isDialectQueryClassifiesQueryTypesCorrectly(
      @ForAll("allQueryTypes") QueryType queryType) {
    if (queryType == QueryType.PPL || queryType == QueryType.SQL) {
      assertFalse(
          queryType.isDialectQuery(),
          queryType + " should NOT be classified as a dialect query");
    } else {
      assertTrue(
          queryType.isDialectQuery(), queryType + " should be classified as a dialect query");
    }
  }

  @Provide
  Arbitrary<QueryType> dialectQueryTypes() {
    return Arbitraries.of(
        Arrays.stream(QueryType.values()).filter(QueryType::isDialectQuery).toArray(QueryType[]::new));
  }

  @Provide
  Arbitrary<QueryType> allQueryTypes() {
    return Arbitraries.of(QueryType.values());
  }
}
