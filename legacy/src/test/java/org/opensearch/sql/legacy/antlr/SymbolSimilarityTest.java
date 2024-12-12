/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** Test cases for symbol similarity */
public class SymbolSimilarityTest {

  @Test
  public void noneCandidateShouldReturnTargetStringItself() {
    String target = "test";
    String mostSimilarSymbol = new SimilarSymbols(emptyList()).mostSimilarTo(target);
    Assert.assertEquals(target, mostSimilarSymbol);
  }

  @Test
  public void singleCandidateShouldReturnTheOnlyCandidate() {
    String target = "test";
    String candidate = "hello";
    String mostSimilarSymbol = new SimilarSymbols(singletonList(candidate)).mostSimilarTo(target);
    Assert.assertEquals(candidate, mostSimilarSymbol);
  }

  @Test
  public void twoCandidatesShouldReturnMostSimilarCandidate() {
    String target = "test";
    String mostSimilar = "tests";
    List<String> candidates = Arrays.asList("hello", mostSimilar);
    String mostSimilarSymbol = new SimilarSymbols(candidates).mostSimilarTo(target);
    Assert.assertEquals(mostSimilar, mostSimilarSymbol);
  }

  @Test
  public void manyCandidatesShouldReturnMostSimilarCandidate() {
    String target = "test";
    String mostSimilar = "tests";
    List<String> candidates = Arrays.asList("hello", mostSimilar, "world");
    String mostSimilarSymbol = new SimilarSymbols(candidates).mostSimilarTo(target);
    Assert.assertEquals(mostSimilar, mostSimilarSymbol);
  }
}
