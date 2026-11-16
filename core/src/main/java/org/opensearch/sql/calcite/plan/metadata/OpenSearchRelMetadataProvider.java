/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;

/**
 * Metadata provider used by the PPL planner: OpenSearch overrides first, then Calcite's defaults.
 */
public final class OpenSearchRelMetadataProvider {

  public static final RelMetadataProvider INSTANCE =
      ChainedRelMetadataProvider.of(
          ImmutableList.of(OpenSearchRelMdCollation.SOURCE, DefaultRelMetadataProvider.INSTANCE));

  private OpenSearchRelMetadataProvider() {}
}
