/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.correctness.tests;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import org.junit.Test;
import org.opensearch.sql.correctness.testset.TestQuerySet;

/**
 * Tests for {@link TestQuerySet}
 */
public class TestQuerySetTest {

  @Test
  public void testQuerySet() {
    TestQuerySet querySet =
        new TestQuerySet("SELECT * FROM accounts\nSELECT * FROM accounts LIMIT 5");
    assertThat(
        querySet,
        contains(
            "SELECT * FROM accounts",
            "SELECT * FROM accounts LIMIT 5"
        )
    );
  }

}
