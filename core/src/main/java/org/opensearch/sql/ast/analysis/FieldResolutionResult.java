/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.analysis;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import lombok.Value;
import org.opensearch.sql.calcite.utils.WildcardUtils;

/** Field resolution result separating regular fields from wildcard patterns. */
@Getter
@EqualsAndHashCode
@ToString
public class FieldResolutionResult {

  @NonNull private final Set<String> regularFields;
  @NonNull private final Wildcard wildcard;

  public FieldResolutionResult(Set<String> regularFields) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcard = NULL_WILDCARD;
  }

  public FieldResolutionResult(Set<String> regularFields, Wildcard wildcard) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcard = wildcard;
  }

  public FieldResolutionResult(Set<String> regularFields, String wildcard) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcard = getWildcard(wildcard);
  }

  private static Wildcard getWildcard(String wildcard) {
    if (wildcard == null || wildcard.isEmpty()) {
      return NULL_WILDCARD;
    } else if (wildcard.equals("*")) {
      return ANY_WILDCARD;
    } else {
      return new SingleWildcard(wildcard);
    }
  }

  public FieldResolutionResult(Set<String> regularFields, Set<String> wildcards) {
    this.regularFields = new HashSet<>(regularFields);
    this.wildcard = createOrWildcard(wildcards);
  }

  private static Wildcard createOrWildcard(Set<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return NULL_WILDCARD;
    }
    if (patterns.size() == 1) {
      return getWildcard(patterns.iterator().next());
    }
    List<Wildcard> wildcards =
        patterns.stream().sorted().map(SingleWildcard::new).collect(Collectors.toList());
    return new OrWildcard(wildcards);
  }

  /** Returns unmodifiable view of regular fields. */
  public Set<String> getRegularFieldsUnmodifiable() {
    return Collections.unmodifiableSet(regularFields);
  }

  /** Checks if result contains any wildcard patterns. */
  public boolean hasWildcards() {
    return wildcard != NULL_WILDCARD;
  }

  /** Checks if result contains partial wildcard patterns (not '*'). */
  public boolean hasPartialWildcards() {
    return wildcard != NULL_WILDCARD && wildcard != ANY_WILDCARD;
  }

  /** Checks if result contains regular fields. */
  public boolean hasRegularFields() {
    return !regularFields.isEmpty();
  }

  /** Creates new result excluding specified fields. */
  public FieldResolutionResult exclude(Collection<String> fields) {
    Set<String> combinedFields = new HashSet<>(this.regularFields);
    combinedFields.removeAll(fields);
    return new FieldResolutionResult(combinedFields, this.wildcard);
  }

  /** Creates new result combining this result with additional fields (union). */
  public FieldResolutionResult or(Set<String> fields) {
    Set<String> combinedFields = new HashSet<>(this.regularFields);
    combinedFields.addAll(fields);
    return new FieldResolutionResult(combinedFields, this.wildcard);
  }

  private Set<String> and(Set<String> fields) {
    return fields.stream()
        .filter(field -> this.getRegularFields().contains(field) || this.wildcard.matches(field))
        .collect(Collectors.toSet());
  }

  /** Creates new result intersecting this result with another (intersection). */
  public FieldResolutionResult and(FieldResolutionResult other) {
    Set<String> combinedFields = new HashSet<>();
    combinedFields.addAll(this.and(other.regularFields));
    combinedFields.addAll(other.and(this.regularFields));

    Wildcard combinedWildcard = this.wildcard.and(other.wildcard);

    return new FieldResolutionResult(combinedFields, combinedWildcard);
  }

  /** Interface for wildcard pattern matching. */
  public interface Wildcard {
    /** Checks if field name matches wildcard pattern. */
    boolean matches(String fieldName);

    default Wildcard and(Wildcard other) {
      return new AndWildcard(this, other);
    }

    default Wildcard or(Wildcard other) {
      return new OrWildcard(this, other);
    }
  }

  static Wildcard ANY_WILDCARD =
      new Wildcard() {
        @Override
        public boolean matches(String fieldName) {
          return true;
        }

        @Override
        public String toString() {
          return "*";
        }

        @Override
        public Wildcard and(Wildcard other) {
          return other;
        }

        @Override
        public Wildcard or(Wildcard other) {
          return this;
        }
      };

  static Wildcard NULL_WILDCARD =
      new Wildcard() {
        public boolean matches(String fieldName) {
          return false;
        }

        @Override
        public String toString() {
          return "";
        }

        @Override
        public Wildcard and(Wildcard other) {
          return this;
        }

        @Override
        public Wildcard or(Wildcard other) {
          return other;
        }
      };

  /** Single wildcard pattern using '*' as wildcard character. */
  @Value
  static class SingleWildcard implements Wildcard {
    String pattern;

    @Override
    public boolean matches(String fieldName) {
      return WildcardUtils.matchesWildcardPattern(pattern, fieldName);
    }

    @Override
    public String toString() {
      return pattern;
    }
  }

  /** OR combination of wildcard patterns (matches if ANY pattern matches). */
  @Value
  static class OrWildcard implements Wildcard {
    List<Wildcard> patterns;

    public OrWildcard(Wildcard... patterns) {
      this.patterns = List.of(patterns);
    }

    public OrWildcard(Collection<Wildcard> patterns) {
      this.patterns = List.copyOf(patterns);
    }

    @Override
    public boolean matches(String fieldName) {
      return patterns.stream().anyMatch(p -> p.matches(fieldName));
    }

    @Override
    public String toString() {
      return patterns.stream().map(Wildcard::toString).collect(Collectors.joining(" | "));
    }

    @Override
    public Wildcard or(Wildcard other) {
      if (other instanceof SingleWildcard) {
        List<Wildcard> newPatterns =
            ImmutableList.<FieldResolutionResult.Wildcard>builder()
                .addAll(patterns)
                .add(other)
                .build();
        return new OrWildcard(newPatterns);
      } else if (other == NULL_WILDCARD) {
        return this;
      } else if (other == ANY_WILDCARD) {
        return ANY_WILDCARD;
      } else {
        return Wildcard.super.or(other);
      }
    }
  }

  /** AND combination of wildcard patterns (matches if ALL patterns match). */
  @Value
  static class AndWildcard implements Wildcard {
    List<Wildcard> patterns;

    public AndWildcard(Wildcard... patterns) {
      this.patterns = List.of(patterns);
    }

    public AndWildcard(Collection<Wildcard> patterns) {
      this.patterns = List.copyOf(patterns);
    }

    @Override
    public boolean matches(String fieldName) {
      return patterns.stream().allMatch(p -> p.matches(fieldName));
    }

    @Override
    public String toString() {
      return patterns.stream()
          .map(p -> "(" + p.toString() + ")")
          .collect(Collectors.joining(" & "));
    }

    @Override
    public Wildcard and(Wildcard other) {
      if (other instanceof SingleWildcard) {
        List<Wildcard> newPatterns =
            ImmutableList.<FieldResolutionResult.Wildcard>builder()
                .addAll(patterns)
                .add(other)
                .build();
        return new AndWildcard(newPatterns);
      } else if (other == NULL_WILDCARD) {
        return NULL_WILDCARD;
      } else if (other == ANY_WILDCARD) {
        return this;
      } else {
        return Wildcard.super.and(other);
      }
    }
  }
}
