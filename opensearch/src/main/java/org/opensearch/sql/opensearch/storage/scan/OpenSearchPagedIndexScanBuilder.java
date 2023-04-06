/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanQueryBuilder.findReferenceExpressions;

import lombok.EqualsAndHashCode;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalHighlight;
import org.opensearch.sql.planner.logical.LogicalProject;
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
  public boolean pushDownFilter(LogicalFilter filter) {
    FilterQueryBuilder queryBuilder = new FilterQueryBuilder(
        new DefaultExpressionSerializer());
    QueryBuilder query = queryBuilder.build(filter.getCondition());
    indexScan.getRequestBuilder().pushDownFilter(query);
    return true;
  }

  @Override
  public boolean pushDownProject(LogicalProject project) {
    indexScan.getRequestBuilder().pushDownProjects(
        findReferenceExpressions(project.getProjectList()));
    return false;
  }

  @Override
  public boolean pushDownHighlight(LogicalHighlight highlight) {
    indexScan.getRequestBuilder().pushDownHighlight(
        StringUtils.unquoteText(highlight.getHighlightField().toString()),
        highlight.getArguments());
    return true;
  }

  @Override
  public TableScanOperator build() {
    return indexScan;
  }
}
