/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.parser;

import static org.junit.Assert.assertEquals;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.sql.legacy.domain.bucketpath.BucketPath;
import org.opensearch.sql.legacy.domain.bucketpath.Path;

public class BucketPathTest {

  @Rule public final ExpectedException exceptionRule = ExpectedException.none();

  private final Path agg1 = Path.getAggPath("projects@NESTED");
  private final Path agg2 = Path.getAggPath("projects@FILTERED");
  private final Path metric = Path.getMetricPath("c");

  @Test
  public void bucketPath() {
    BucketPath bucketPath = new BucketPath();
    bucketPath.add(metric);
    bucketPath.add(agg2);
    bucketPath.add(agg1);

    assertEquals("projects@NESTED>projects@FILTERED.c", bucketPath.getBucketPath());
  }

  @Test
  public void bucketPathEmpty() {
    BucketPath bucketPath = new BucketPath();

    assertEquals("", bucketPath.getBucketPath());
  }

  @Test
  public void theLastMustBeMetric() {
    BucketPath bucketPath = new BucketPath();

    exceptionRule.expect(AssertionError.class);
    exceptionRule.expectMessage("The last path in the bucket path must be Metric");
    bucketPath.add(agg1);
  }

  @Test
  public void allTheOtherMustBeAgg() {
    BucketPath bucketPath = new BucketPath();

    exceptionRule.expect(AssertionError.class);
    exceptionRule.expectMessage("All the other path in the bucket path must be Agg");
    bucketPath.add(metric);
    bucketPath.add(metric);
  }
}
