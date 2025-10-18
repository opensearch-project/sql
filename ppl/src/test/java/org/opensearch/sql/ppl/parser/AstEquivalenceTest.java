/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.parser;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.opensearch.sql.ppl.AstPlanningTestBase;

public class AstEquivalenceTest extends AstPlanningTestBase {
  @Test
  public void testSpathArgumentDeshuffle() {
    assertEquals(plan("source = t | spath path=a input=a"), plan("source = t | spath input=a a"));
  }

  @Test
  public void testHeadLimitEquivalent() {
    assertEquals(plan("source = t | head limit=50"), plan("source = t | head 50"));
  }

  @Test
  public void testTopLimitEquivalent() {
    assertEquals(
        plan("source = t | top limit=50 field_name"), plan("source = t | top 50 field_name"));
  }
}
