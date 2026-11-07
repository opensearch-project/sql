/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.planner.logical.rules;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PrometheusPushDownContextTest {

  @Test
  void testDefaultState() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    assertFalse(ctx.isTimeRangePushed());
    assertFalse(ctx.isLabelFilterPushed());
    assertTrue(ctx.getLabelMatchers().isEmpty());
    assertNull(ctx.getStartTime());
    assertNull(ctx.getEndTime());
    assertEquals("14", ctx.getEffectiveStep());
  }

  @Test
  void testPushLabelMatcher() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    ctx.pushLabelMatcher("service", "frontend");

    assertTrue(ctx.isLabelFilterPushed());
    assertEquals(1, ctx.getLabelMatchers().size());
    assertEquals("frontend", ctx.getLabelMatchers().get("service"));
  }

  @Test
  void testPushMultipleLabels() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    ctx.pushLabelMatcher("service", "frontend");
    ctx.pushLabelMatcher("job", "prometheus");

    assertEquals(2, ctx.getLabelMatchers().size());
    assertEquals("frontend", ctx.getLabelMatchers().get("service"));
    assertEquals("prometheus", ctx.getLabelMatchers().get("job"));
  }

  @Test
  void testPushStartTime() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    ctx.pushStartTime(1700000000L);

    assertTrue(ctx.isTimeRangePushed());
    assertEquals(1700000000L, ctx.getStartTime());
    assertEquals(1700000000L, ctx.getEffectiveStartTime());
  }

  @Test
  void testPushEndTime() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    ctx.pushEndTime(1700003600L);

    assertTrue(ctx.isTimeRangePushed());
    assertEquals(1700003600L, ctx.getEndTime());
    assertEquals(1700003600L, ctx.getEffectiveEndTime());
  }

  @Test
  void testEffectiveStartTimeDefault() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    long effectiveStart = ctx.getEffectiveStartTime();
    long now = java.time.Instant.now().getEpochSecond();

    // Default is now - 3600 seconds (1 hour), allow 5s tolerance
    assertTrue(Math.abs(effectiveStart - (now - 3600)) < 5,
        "Default start time should be approximately now - 1 hour");
  }

  @Test
  void testEffectiveEndTimeDefault() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    long effectiveEnd = ctx.getEffectiveEndTime();
    long now = java.time.Instant.now().getEpochSecond();

    // Default is now, allow 5s tolerance
    assertTrue(Math.abs(effectiveEnd - now) < 5,
        "Default end time should be approximately now");
  }

  @Test
  void testSetStep() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    ctx.setStep("30");

    assertEquals("30", ctx.getEffectiveStep());
  }

  @Test
  void testGetEffectiveStepWhenNull() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    ctx.setStep(null);

    assertEquals("14", ctx.getEffectiveStep());
  }

  @Test
  void testBuildPromQLNoLabels() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    assertEquals("up", ctx.buildPromQL("up"));
    assertEquals("http_requests_total", ctx.buildPromQL("http_requests_total"));
  }

  @Test
  void testBuildPromQLSingleLabel() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    ctx.pushLabelMatcher("job", "prometheus");

    assertEquals("up{job=\"prometheus\"}", ctx.buildPromQL("up"));
  }

  @Test
  void testBuildPromQLMultipleLabels() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    ctx.pushLabelMatcher("job", "prometheus");
    ctx.pushLabelMatcher("instance", "localhost:9090");

    assertEquals(
        "up{job=\"prometheus\",instance=\"localhost:9090\"}",
        ctx.buildPromQL("up"));
  }

  @Test
  void testBuildPromQLEscapesDoubleQuotes() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    ctx.pushLabelMatcher("path", "/api/v1/\"test\"");

    assertEquals(
        "metric{path=\"/api/v1/\\\"test\\\"\"}",
        ctx.buildPromQL("metric"));
  }

  @Test
  void testBuildPromQLEscapesBackslashes() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    ctx.pushLabelMatcher("path", "C:\\Users\\admin");

    assertEquals(
        "metric{path=\"C:\\\\Users\\\\admin\"}",
        ctx.buildPromQL("metric"));
  }

  @Test
  void testBuildPromQLEscapesNewlines() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    ctx.pushLabelMatcher("msg", "line1\nline2");

    assertEquals(
        "metric{msg=\"line1\\nline2\"}",
        ctx.buildPromQL("metric"));
  }

  @Test
  void testBuildPromQLEscapesCombined() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    // Value with all special chars: backslash, quote, newline
    ctx.pushLabelMatcher("val", "a\\b\"c\nd");

    assertEquals(
        "metric{val=\"a\\\\b\\\"c\\nd\"}",
        ctx.buildPromQL("metric"));
  }

  @Test
  void testBuildPromQLInjectionAttempt() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    // Attempt to break out of label value and inject another matcher
    ctx.pushLabelMatcher("service", "frontend\"} OR {__name__=\"secret");

    String result = ctx.buildPromQL("metric");
    // The injection should be safely escaped inside the quotes
    assertEquals(
        "metric{service=\"frontend\\\"} OR {__name__=\\\"secret\"}",
        result);
  }

  @Test
  void testEscapePromQLLabelValue() {
    assertEquals("simple", PrometheusPushDownContext.escapePromQLLabelValue("simple"));
    assertEquals("has\\\\backslash", PrometheusPushDownContext.escapePromQLLabelValue("has\\backslash"));
    assertEquals("has\\\"quote", PrometheusPushDownContext.escapePromQLLabelValue("has\"quote"));
    assertEquals("has\\nnewline", PrometheusPushDownContext.escapePromQLLabelValue("has\nnewline"));
    assertEquals("", PrometheusPushDownContext.escapePromQLLabelValue(""));
  }

  @Test
  void testCopyIsIndependent() {
    PrometheusPushDownContext original = new PrometheusPushDownContext();
    original.pushLabelMatcher("service", "frontend");
    original.pushStartTime(1700000000L);
    original.setStep("30");

    PrometheusPushDownContext copy = original.copy();

    // Copy has same state
    assertEquals(original.getLabelMatchers(), copy.getLabelMatchers());
    assertEquals(original.getStartTime(), copy.getStartTime());
    assertEquals(original.getEffectiveStep(), copy.getEffectiveStep());
    assertTrue(copy.isLabelFilterPushed());
    assertTrue(copy.isTimeRangePushed());

    // Mutating copy doesn't affect original
    copy.pushLabelMatcher("job", "node");
    copy.pushEndTime(1700003600L);
    copy.setStep("60");

    assertEquals(1, original.getLabelMatchers().size());
    assertNull(original.getEndTime());
    assertEquals("30", original.getEffectiveStep());
    assertNotSame(original.getLabelMatchers(), copy.getLabelMatchers());
  }

  @Test
  void testToStringEmpty() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();

    assertEquals("[]", ctx.toString());
  }

  @Test
  void testToStringWithLabels() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    ctx.pushLabelMatcher("service", "frontend");

    assertEquals("[LABELS->{service=frontend}]", ctx.toString());
  }

  @Test
  void testToStringWithTimeRange() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    ctx.pushStartTime(1700000000L);
    ctx.pushEndTime(1700003600L);

    assertEquals("[TIME_RANGE->[1700000000,1700003600]]", ctx.toString());
  }

  @Test
  void testToStringWithLabelsAndTimeRange() {
    PrometheusPushDownContext ctx = new PrometheusPushDownContext();
    ctx.pushLabelMatcher("service", "frontend");
    ctx.pushStartTime(1700000000L);
    ctx.pushEndTime(1700003600L);

    assertEquals("[LABELS->{service=frontend}, TIME_RANGE->[1700000000,1700003600]]", ctx.toString());
  }
}
