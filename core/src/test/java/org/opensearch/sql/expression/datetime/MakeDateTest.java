/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;

import java.time.LocalDate;
import java.time.Year;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.expression.DSL;

public class MakeDateTest extends DateTimeTestBase {

  @Test
  public void checkEdgeCases() {
    assertEquals(
        LocalDate.ofYearDay(2002, 1),
        makedate(2001., 366.),
        "No switch to the next year on getting 366th day of a non-leap year");
    assertEquals(
        LocalDate.ofYearDay(2005, 1),
        makedate(2004., 367.),
        "No switch to the next year on getting 367th day of a leap year");
    assertEquals(
        LocalDate.ofYearDay(2000, 42),
        makedate(0., 42.),
        "0 year is not interpreted as 2000 as in MySQL");
    assertEquals(
        nullValue(),
        eval(makedate(DSL.literal(-1.), DSL.literal(42.))),
        "Negative year doesn't produce NULL");
    assertEquals(
        nullValue(),
        eval(makedate(DSL.literal(42.), DSL.literal(-1.))),
        "Negative dayOfYear doesn't produce NULL");
    assertEquals(
        nullValue(),
        eval(makedate(DSL.literal(42.), DSL.literal(0.))),
        "Zero dayOfYear doesn't produce NULL");

    assertEquals(LocalDate.of(1999, 3, 1), makedate(1999., 60.), "Got Feb 29th of a non-lear year");
    assertEquals(LocalDate.of(1999, 12, 31), makedate(1999., 365.));
    assertEquals(LocalDate.of(2004, 12, 31), makedate(2004., 366.));
  }

  @Test
  public void checkRounding() {
    assertEquals(LocalDate.of(42, 1, 1), makedate(42.49, 1.49));
    assertEquals(LocalDate.of(43, 1, 2), makedate(42.50, 1.50));
  }

  private static Stream<Arguments> getTestData() {
    return Stream.of(
        Arguments.of(3755.421154, 9.300720),
        Arguments.of(3416.922084, 850.832172),
        Arguments.of(498.717527, 590.831215),
        Arguments.of(1255.402786, 846.041171),
        Arguments.of(2491.200868, 832.929840),
        Arguments.of(1140.775582, 345.592629),
        Arguments.of(2087.208382, 110.392189),
        Arguments.of(4582.515870, 763.629197),
        Arguments.of(1654.431245, 476.360251),
        Arguments.of(1342.494306, 70.108352),
        Arguments.of(171.841206, 794.470738),
        Arguments.of(5000.103926, 441.461842),
        Arguments.of(2957.828371, 273.909052),
        Arguments.of(2232.699033, 171.537097),
        Arguments.of(4650.163672, 226.857148),
        Arguments.of(495.943520, 735.062451),
        Arguments.of(4568.187019, 552.394124),
        Arguments.of(688.085482, 283.574200),
        Arguments.of(4627.662672, 791.729059),
        Arguments.of(2812.837393, 397.688304),
        Arguments.of(3050.030341, 596.714966),
        Arguments.of(3617.452566, 619.795467),
        Arguments.of(2210.322073, 106.914268),
        Arguments.of(675.757974, 147.702828),
        Arguments.of(1101.801820, 40.055318));
  }

  /**
   * Test function with given pseudo-random values.
   *
   * @param year year
   * @param dayOfYear day of year
   */
  @ParameterizedTest(name = "year = {0}, dayOfYear = {1}")
  @MethodSource("getTestData")
  public void checkRandomValues(double year, double dayOfYear) {
    LocalDate actual = makedate(year, dayOfYear);
    LocalDate expected = getReferenceValue(year, dayOfYear);

    assertEquals(expected, actual, String.format("year = %f, dayOfYear = %f", year, dayOfYear));
  }

  /**
   * Using another algorithm to get reference value. We should go to the next year until
   * remaining @dayOfYear is bigger than 365/366.
   *
   * @param year Year.
   * @param dayOfYear Day of the year.
   * @return The calculated date.
   */
  private LocalDate getReferenceValue(double year, double dayOfYear) {
    var yearL = (int) Math.round(year);
    var dayL = (int) Math.round(dayOfYear);
    while (true) {
      int daysInYear = Year.isLeap(yearL) ? 366 : 365;
      if (dayL > daysInYear) {
        dayL -= daysInYear;
        yearL++;
      } else {
        break;
      }
    }
    return LocalDate.ofYearDay(yearL, dayL);
  }
}
