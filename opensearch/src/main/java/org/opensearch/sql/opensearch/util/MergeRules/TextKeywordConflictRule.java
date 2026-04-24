/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util.MergeRules;

import java.util.Map;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchTextType;

/**
 * Merge rule for text/keyword type conflicts across indices. When a field is text in one index and
 * keyword in another, or text-with-keyword-subfield in one and text-without in another, we merge to
 * text WITHOUT keyword subfields. This forces _source retrieval instead of doc_values, which works
 * universally across all shards regardless of the actual field type.
 *
 * <p>See GitHub issue #4659.
 */
public class TextKeywordConflictRule implements MergeRule {

  @Override
  public boolean isMatch(OpenSearchDataType source, OpenSearchDataType target) {
    if (source == null || target == null) {
      return false;
    }
    MappingType sourceMapping = source.getMappingType();
    MappingType targetMapping = target.getMappingType();
    if (sourceMapping == null || targetMapping == null) {
      return false;
    }
    // Match when one is text and the other is keyword
    if (isTextLike(sourceMapping) && isKeyword(targetMapping)) {
      return true;
    }
    if (isKeyword(sourceMapping) && isTextLike(targetMapping)) {
      return true;
    }
    // Match when both are text but one has keyword subfields and the other does not
    if (isTextLike(sourceMapping) && isTextLike(targetMapping)) {
      boolean sourceHasKeywordSub = hasKeywordSubField(source);
      boolean targetHasKeywordSub = hasKeywordSubField(target);
      return sourceHasKeywordSub != targetHasKeywordSub;
    }
    return false;
  }

  @Override
  public void mergeInto(
      String key, OpenSearchDataType source, Map<String, OpenSearchDataType> target) {
    // Always merge to text WITHOUT keyword subfields.
    // This forces _source retrieval, which works for both text and keyword fields.
    target.put(key, OpenSearchTextType.of());
  }

  private static boolean isTextLike(MappingType mappingType) {
    return mappingType == MappingType.Text || mappingType == MappingType.MatchOnlyText;
  }

  private static boolean isKeyword(MappingType mappingType) {
    return mappingType == MappingType.Keyword;
  }

  private static boolean hasKeywordSubField(OpenSearchDataType type) {
    if (type instanceof OpenSearchTextType textType) {
      return textType.getFields().values().stream()
          .anyMatch(f -> f.getMappingType() == MappingType.Keyword);
    }
    return false;
  }
}
