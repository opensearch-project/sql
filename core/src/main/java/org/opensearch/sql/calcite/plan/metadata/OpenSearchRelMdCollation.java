/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.plan.metadata;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Collation metadata that overrides {@link RelMdCollation} for {@link Values}.
 *
 * <p>For a {@code Values} with no rows, {@link RelMdCollation#values} treats every column prefix as
 * sorted and returns n collations of length n, n-1, ..., 1, which is O(n^2) in the number of
 * fields. Every {@code Project} above it then maps all of them, which is O(n^3). An always-false
 * filter on a wide index is replaced by such a {@code Values}, so planning could take minutes and
 * exhaust the heap (opensearch-project/sql#5952). An empty input has no meaningful sort order, so
 * we report no collations, which is always a valid (conservative) answer.
 */
public class OpenSearchRelMdCollation implements MetadataHandler<BuiltInMetadata.Collation> {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new OpenSearchRelMdCollation(), BuiltInMetadata.Collation.Handler.class);

  protected OpenSearchRelMdCollation() {}

  @Override
  public MetadataDef<BuiltInMetadata.Collation> getDef() {
    return BuiltInMetadata.Collation.DEF;
  }

  /** Same as {@link RelMdCollation#collations(Values, RelMetadataQuery)}, except for 0 rows. */
  public ImmutableList<RelCollation> collations(Values values, RelMetadataQuery mq) {
    if (values.getTuples().isEmpty()) {
      return ImmutableList.of();
    }
    return ImmutableList.copyOf(RelMdCollation.values(mq, values.getRowType(), values.getTuples()));
  }
}
