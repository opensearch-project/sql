/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.rewriter.alias;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import org.junit.Assert;
import org.junit.Test;

/** Test cases for util class {@link Identifier}. */
public class IdentifierTest {

  @Test
  public void identifierWithWordBeforeFirstDotShouldBeConsideredHavePrefix() {
    Assert.assertTrue(identifier("accounts.age").hasPrefix());
  }

  @Test
  public void identifierWithoutDotShouldNotBeConsideredHavePrefix() {
    Assert.assertFalse(identifier("age").hasPrefix());
  }

  @Test
  public void identifierStartingWithDotShouldNotBeConsideredHavePrefix() {
    Assert.assertFalse(identifier(".age").hasPrefix());
  }

  @Test
  public void prefixOfIdentifierShouldBeWordBeforeFirstDot() {
    Assert.assertEquals("accounts", identifier("accounts.age").prefix());
  }

  @Test
  public void removePrefixShouldRemoveFirstWordAndDot() {
    Identifier identifier = identifier("accounts.age");
    identifier.removePrefix();
    Assert.assertEquals("age", identifier.name());
  }

  private Identifier identifier(String name) {
    return new Identifier(new SQLIdentifierExpr(name));
  }
}
