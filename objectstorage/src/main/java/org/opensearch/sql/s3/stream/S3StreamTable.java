/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.s3.stream;


import com.google.common.annotations.VisibleForTesting;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.s3.storage.OSS3Object;
import org.opensearch.sql.s3.storage.S3ScanOperator;
import org.opensearch.sql.s3.storage.S3Table;

/**
 * s3 stream table.
 */
public class S3StreamTable extends S3Table {

  private final Set<OSS3Object> files;

  private final String location;

  public S3StreamTable(Set<OSS3Object> files, S3Table table) {
    super(
        table.getTableName(), table.getColNameTypes(), table.getFileFormat(), table.getLocation());
    this.location = getLocation();
    this.files = files;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new S3PlanImplementor(), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class S3PlanImplementor extends DefaultImplementor<Void> {
    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Void context) {
      return new S3ScanOperator(location, files);
    }
  }
}
