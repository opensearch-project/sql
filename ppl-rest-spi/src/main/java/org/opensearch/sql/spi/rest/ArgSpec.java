/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The query args a {@code rest} endpoint accepts, and the allowed value domain of each. This is the
 * per-endpoint half of the allow-list: an arg the endpoint does not declare is rejected, and a
 * declared arg whose value falls outside its domain is rejected. Both checks are enforced uniformly
 * by the sql registry, so a provider expresses its policy as data rather than as code.
 *
 * <p>An arg declared with an empty domain accepts any value. An arg declared as a {@code csvArg}
 * validates each comma-separated token against the domain (e.g. {@code expand_wildcards}).
 */
public final class ArgSpec {

  public static final ArgSpec NONE = builder().build();

  // arg name -> allowed values (empty set means "any value allowed").
  private final Map<String, Set<String>> valueDomains;
  // subset of arg names whose value is a comma-separated list validated token-by-token.
  private final Set<String> csvArgs;

  private ArgSpec(Map<String, Set<String>> valueDomains, Set<String> csvArgs) {
    this.valueDomains = valueDomains;
    this.csvArgs = csvArgs;
  }

  public Set<String> allowedArgs() {
    return valueDomains.keySet();
  }

  public boolean allows(String arg) {
    return valueDomains.containsKey(arg);
  }

  /**
   * Validate the value of an accepted arg against its domain. No-op if the arg has no domain (any
   * value allowed). Throws {@link IllegalArgumentException} with a clear client error otherwise.
   */
  public void validateValue(String endpoint, String arg, String value) {
    Set<String> domain = valueDomains.get(arg);
    if (domain == null || domain.isEmpty()) {
      return;
    }
    if (csvArgs.contains(arg)) {
      if (value == null || value.isBlank()) {
        throw unsupported(endpoint, arg, value, domain);
      }
      for (String token : value.toLowerCase(Locale.ROOT).split(",")) {
        if (!domain.contains(token.trim())) {
          throw unsupported(endpoint, arg, value, domain);
        }
      }
    } else if (value == null || !domain.contains(value.toLowerCase(Locale.ROOT))) {
      throw unsupported(endpoint, arg, value, domain);
    }
  }

  private static IllegalArgumentException unsupported(
      String endpoint, String arg, String value, Set<String> domain) {
    return new IllegalArgumentException(
        "rest endpoint ["
            + endpoint
            + "] arg ["
            + arg
            + "] has an unsupported value ["
            + value
            + "]. Allowed values: "
            + domain);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link ArgSpec}. Declaration order is preserved for stable error messages. */
  public static final class Builder {
    private final LinkedHashMap<String, Set<String>> valueDomains = new LinkedHashMap<>();
    private final Set<String> csvArgs = new LinkedHashSet<>();

    public Builder arg(String name) {
      valueDomains.put(name, Set.of());
      return this;
    }

    public Builder arg(String name, Set<String> domain) {
      valueDomains.put(name, Set.copyOf(domain));
      return this;
    }

    /** Accept {@code name} as a comma-separated list; every token must be in {@code domain}. */
    public Builder csvArg(String name, Set<String> domain) {
      valueDomains.put(name, Set.copyOf(domain));
      csvArgs.add(name);
      return this;
    }

    public ArgSpec build() {
      return new ArgSpec(new LinkedHashMap<>(valueDomains), Set.copyOf(csvArgs));
    }
  }
}
