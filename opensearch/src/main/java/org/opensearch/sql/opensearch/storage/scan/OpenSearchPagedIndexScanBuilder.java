/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import lombok.EqualsAndHashCode;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Builder for a paged OpenSearch request.
 * Override pushDown* methods from TableScanBuilder as more features
 * support pagination.
 */
public class OpenSearchPagedIndexScanBuilder extends TableScanBuilder {
  @EqualsAndHashCode.Include
  OpenSearchPagedIndexScan indexScan;

  public OpenSearchPagedIndexScanBuilder(OpenSearchPagedIndexScan indexScan) {
    this.indexScan = indexScan;
  }

  @Override
  public TableScanOperator build() {
    return indexScan;
  }
}
