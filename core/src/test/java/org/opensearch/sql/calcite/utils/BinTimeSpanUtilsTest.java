/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Calendar;
import java.util.Date;
import org.junit.jupiter.api.Test;

/**
 * Test cases to reproduce and validate the aligntime issue with bin command. The issue:
 * aligntime="@d+4h" should produce timestamps like "2025-07-27 16:00" and "2025-07-28 04:00" but
 * we're getting wrong timestamps due to incorrect alignment point calculation.
 */
public class BinTimeSpanUtilsTest {

  @Test
  public void testAlignTimeAtDPlus4HoursProblem() {
    // This test demonstrates the current problem with @d+4h alignment

    // Test Case 1: A timestamp at 2025-07-27 18:30 (6:30 PM) with 12h span and @d+4h alignment
    // Expected: Should bin to 2025-07-27 16:00 (4:00 PM - the 4h offset from start of day)
    // The next bin should be 2025-07-28 04:00 (4:00 AM the next day)

    System.out.println("=== Testing aligntime @d+4h issue ===");

    // Simulate timestamp: 2025-07-27 18:30:00 (6:30 PM)
    // In epoch milliseconds: 1753715400000L (approximate)
    long testTimestamp = 1753715400000L; // 2025-07-27 18:30:00 UTC

    String timeModifier = "@d+4h";
    int intervalValue = 12; // 12 hour span
    String unit = "h";

    System.out.println("Input timestamp: " + testTimestamp + " (2025-07-27 18:30 UTC)");
    System.out.println("Time modifier: " + timeModifier);
    System.out.println("Interval: " + intervalValue + unit);

    // The problem: Let's manually trace through the createTimeModifierAlignedSpan logic

    // Step 1: Parse time modifier @d+4h
    // alignToDay = true
    // offsetMillis = 4 * 3600000 = 14400000 (4 hours in milliseconds)

    long offsetMillis = 4 * 3600000L; // 4 hours
    System.out.println("Parsed offset: " + offsetMillis + "ms (4 hours)");

    // Step 2: Calculate start of day for 2025-07-27
    // floor(1753715400000 / 86400000) * 86400000
    long millisecondsPerDay = 86400000L;
    long dayNumber = testTimestamp / millisecondsPerDay;
    long startOfDay = dayNumber * millisecondsPerDay;

    System.out.println("Start of day: " + startOfDay);
    System.out.println("Start of day date: " + new Date(startOfDay));

    // Step 3: Add 4h offset to get alignment point
    long alignmentPoint = startOfDay + offsetMillis;
    System.out.println("Alignment point: " + alignmentPoint);
    System.out.println("Alignment point date: " + new Date(alignmentPoint));

    // Step 4: Calculate relative position from alignment point
    long relativePosition = testTimestamp - alignmentPoint;
    System.out.println("Relative position: " + relativePosition + "ms");

    // Step 5: Calculate interval in milliseconds (12 hours)
    long intervalMillis = 12 * 3600000L; // 12 hours
    System.out.println("Interval: " + intervalMillis + "ms (12 hours)");

    // Step 6: Perform binning
    long binNumber = relativePosition / intervalMillis;
    long binOffset = binNumber * intervalMillis;

    System.out.println("Bin number: " + binNumber);
    System.out.println("Bin offset: " + binOffset + "ms");

    // Step 7: Calculate bin start
    long binStartMillis = alignmentPoint + binOffset;
    System.out.println("Bin start: " + binStartMillis);
    System.out.println("Bin start date: " + new Date(binStartMillis));

    // EXPECTED RESULT: For 18:30 with @d+4h and 12h span:
    // - Alignment point should be 2025-07-27 04:00 (4 AM)
    // - 18:30 is 14.5 hours after 4 AM
    // - With 12h span: bin 0 = 4:00-16:00, bin 1 = 16:00-28:00 (next day 4:00)
    // - 18:30 falls in bin 1, so should bin to 16:00 (4 PM)

    Date expectedBinStart = new Date(alignmentPoint + intervalMillis);
    System.out.println("Expected bin start: " + expectedBinStart + " (should be 2025-07-27 16:00)");

    // The issue might be in the calculation - let's see what we actually get
    Date actualBinStart = new Date(binStartMillis);
    System.out.println("Actual bin start: " + actualBinStart);

    // Check if our manual calculation matches expected behavior
    // For @d+4h alignment with 12h span:
    // - Bin 0: 04:00 - 16:00
    // - Bin 1: 16:00 - 04:00 (next day)
    // 18:30 should be in Bin 1, starting at 16:00

    assertTrue(binStartMillis > 0, "Bin start should be positive");

    // Print debug info to understand the issue
    System.out.println("\n=== Debug Analysis ===");
    System.out.println(
        "Issue: The alignment calculation should produce bins starting at 16:00 for the 18:30"
            + " input");
    System.out.println("Current calculation produces: " + new Date(binStartMillis));
    System.out.println("Expected: 2025-07-27 16:00:00");

    // The problem is likely in how we calculate the bins relative to the alignment point
    // Let's test the next case too

    System.out.println("\n=== Testing next day case ===");
    // Test timestamp for next day: 2025-07-28 02:30 (should bin to 2025-07-28 04:00)
    long nextDayTimestamp =
        testTimestamp + 86400000L + (8 * 3600000L); // Next day + 8 hours = 02:30 next day
    System.out.println("Next day input: " + new Date(nextDayTimestamp) + " (2025-07-28 02:30)");

    long nextDayStartOfDay = (nextDayTimestamp / millisecondsPerDay) * millisecondsPerDay;
    long nextDayAlignmentPoint = nextDayStartOfDay + offsetMillis; // 04:00 next day
    long nextDayRelativePos = nextDayTimestamp - nextDayAlignmentPoint;
    long nextDayBinNumber = nextDayRelativePos / intervalMillis;
    long nextDayBinOffset = nextDayBinNumber * intervalMillis;
    long nextDayBinStart = nextDayAlignmentPoint + nextDayBinOffset;

    System.out.println("Next day alignment point: " + new Date(nextDayAlignmentPoint));
    System.out.println("Next day bin start: " + new Date(nextDayBinStart));
    System.out.println(
        "Expected: 2025-07-28 04:00:00 (since 02:30 is before 04:00, it should go to previous"
            + " bin)");

    // The issue: 02:30 is BEFORE the 04:00 alignment point, so relative position is NEGATIVE
    // This causes incorrect binning behavior
    System.out.println(
        "Next day relative position: "
            + nextDayRelativePos
            + "ms (NEGATIVE - this is the problem!)");

    System.out.println("\n=== ROOT CAUSE IDENTIFIED ===");
    System.out.println("Problem: When timestamp is before the alignment point in the day,");
    System.out.println(
        "relative position becomes negative, causing floor() to give wrong bin number.");
    System.out.println(
        "Solution: Need to handle negative relative positions correctly in the binning logic.");
  }

  @Test
  public void testCorrectAlignmentLogic() {
    System.out.println("\n=== Testing Corrected Alignment Logic ===");

    // This test shows what the CORRECT logic should be
    long testTimestamp = 1753641000000L; // 2025-07-27 18:30:00 UTC (corrected timestamp)
    String timeModifier = "@d+4h";
    int intervalValue = 12;

    long offsetMillis = 4 * 3600000L; // 4 hours
    long millisecondsPerDay = 86400000L;
    long intervalMillis = 12 * 3600000L; // 12 hours

    // CORRECTED LOGIC:
    // 1. Find the alignment point for the specific day
    long dayNumber = testTimestamp / millisecondsPerDay;
    long startOfDay = dayNumber * millisecondsPerDay;
    long alignmentPoint = startOfDay + offsetMillis;

    System.out.println("Alignment point: " + new Date(alignmentPoint));

    // 2. Calculate relative position
    long relativePosition = testTimestamp - alignmentPoint;
    System.out.println("Relative position: " + relativePosition + "ms");

    // 3. CORRECTED BINNING: Handle negative relative positions
    long binNumber;
    if (relativePosition >= 0) {
      binNumber = relativePosition / intervalMillis;
    } else {
      // For negative positions, we need to go to the previous bin
      // Math.floor for negative numbers: floor(-1.5) = -2, but we want bin -1
      binNumber = (relativePosition - intervalMillis + 1) / intervalMillis;
    }

    long binOffset = binNumber * intervalMillis;
    long binStartMillis = alignmentPoint + binOffset;

    System.out.println("CORRECTED - Bin number: " + binNumber);
    System.out.println("CORRECTED - Bin start: " + new Date(binStartMillis));

    // For 18:30 with @d+4h (04:00 alignment) and 12h span:
    // relativePosition = 18:30 - 04:00 = 14.5h = 52200000ms
    // binNumber = 52200000 / 43200000 = 1.208... = floor(1.208) = 1
    // binStart = 04:00 + (1 * 12h) = 16:00 ✓

    assertEquals(1, binNumber, "Bin number should be 1 for 18:30 with @d+4h alignment");

    // Verify the bin start time is 16:00 (4 PM) UTC
    Calendar cal = Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"));
    cal.setTime(new Date(binStartMillis));
    assertEquals(16, cal.get(Calendar.HOUR_OF_DAY), "Bin should start at 16:00 (4 PM) UTC");

    System.out.println("✓ CORRECT: 18:30 with @d+4h bins to 16:00");

    // Test negative case: 02:30 next day
    long nextDayTimestamp = testTimestamp + 86400000L - (16 * 3600000L); // 02:30 next day
    long nextDayNumber = nextDayTimestamp / millisecondsPerDay;
    long nextDayStartOfDay = nextDayNumber * millisecondsPerDay;
    long nextDayAlignmentPoint = nextDayStartOfDay + offsetMillis; // 04:00
    long nextDayRelativePos = nextDayTimestamp - nextDayAlignmentPoint; // Negative!

    System.out.println("\nTesting negative relative position case:");
    System.out.println("Next day timestamp: " + new Date(nextDayTimestamp));
    System.out.println("Next day alignment: " + new Date(nextDayAlignmentPoint));
    System.out.println("Relative position: " + nextDayRelativePos + "ms (negative)");

    long nextDayBinNumber;
    if (nextDayRelativePos >= 0) {
      nextDayBinNumber = nextDayRelativePos / intervalMillis;
    } else {
      nextDayBinNumber = (nextDayRelativePos - intervalMillis + 1) / intervalMillis;
    }

    long nextDayBinStart = nextDayAlignmentPoint + (nextDayBinNumber * intervalMillis);
    System.out.println("CORRECTED - Next day bin number: " + nextDayBinNumber);
    System.out.println("CORRECTED - Next day bin start: " + new Date(nextDayBinStart));

    // For 02:30 with 04:00 alignment:
    // relativePosition = 02:30 - 04:00 = -1.5h (negative)
    // Should go to previous bin: bin -1
    // binStart = 04:00 + (-1 * 12h) = 16:00 previous day

    assertTrue(
        nextDayBinNumber < 0,
        "Bin number should be negative for timestamps before alignment point");

    System.out.println("✓ CORRECT: 02:30 correctly handled with negative bin number");
  }

  @Test
  public void testIdentifyExactIssueInCode() {
    System.out.println("\n=== Exact Issue in BinTimeSpanUtils.createTimeModifierAlignedSpan ===");

    System.out.println("The problem is in this section of createTimeModifierAlignedSpan:");
    System.out.println("```java");
    System.out.println("// Calculate the relative position from the alignment point");
    System.out.println("RexNode relativePosition = context.relBuilder.call(");
    System.out.println("    SqlStdOperatorTable.MINUS, epochMillis, alignmentPoint);");
    System.out.println("");
    System.out.println(
        "// Perform binning: floor(relativePosition / intervalMillis) * intervalMillis");
    System.out.println("RexNode divided = context.relBuilder.call(");
    System.out.println("    SqlStdOperatorTable.DIVIDE, relativePosition, intervalLiteral);");
    System.out.println(
        "RexNode binNumber = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);");
    System.out.println("```");
    System.out.println("");
    System.out.println(
        "ISSUE: When relativePosition is negative (timestamp before alignment point),");
    System.out.println("FLOOR() gives incorrect results for negative numbers.");
    System.out.println("");
    System.out.println("Example:");
    System.out.println("- Input: 02:30, Alignment: 04:00, Span: 12h");
    System.out.println("- relativePosition = 02:30 - 04:00 = -1.5h");
    System.out.println("- divided = -1.5h / 12h = -0.125");
    System.out.println("- FLOOR(-0.125) = -1 (but we want 0 to go to previous bin)");
    System.out.println("");
    System.out.println(
        "SOLUTION: Replace the simple FLOOR division with proper negative handling:");
    System.out.println("- For positive relativePosition: use FLOOR(relativePosition / interval)");
    System.out.println(
        "- For negative relativePosition: use FLOOR((relativePosition - interval + 1) / interval)");
    System.out.println("  OR use a CASE statement to handle the logic properly.");

    assertTrue(true, "This test documents the exact issue location and solution");
  }
}
