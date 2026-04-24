/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr.suggestion;

import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.misc.IntervalSet;

/** Fallback provider: surface the parser's expected-tokens set. */
public class ExpectedTokensSuggestionProvider implements SyntaxErrorSuggestionProvider {
  private static final int MAX = 5;

  @Override
  public List<String> getSuggestions(SyntaxErrorContext ctx) {
    RecognitionException e = ctx.getException();
    if (e == null) return List.of();
    IntervalSet expected = e.getExpectedTokens();
    if (expected == null || expected.size() == 0) return List.of();
    List<Integer> types = expected.toList();
    if (types.isEmpty()) return List.of();
    Vocabulary vocab = ctx.getRecognizer().getVocabulary();
    List<String> names = new ArrayList<>(MAX);
    for (int type : types.subList(0, Math.min(types.size(), MAX))) {
      names.add(vocab.getDisplayName(type));
    }
    String msg =
        types.size() > MAX
            ? String.format(
                "Expected one of %d possible tokens. Examples: %s",
                types.size(), String.join(", ", names))
            : "Expected tokens: " + String.join(", ", names);
    return List.of(msg);
  }

  @Override
  public int getPriority() {
    return Integer.MAX_VALUE;
  }
}
