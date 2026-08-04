/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spi.rest;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The query args a {@code rest} endpoint accepts, with the allowed value domain of each. An
 * undeclared arg is rejected; a declared arg whose value is outside its domain is rejected; an
 * empty domain accepts any value.
 */
public final class ArgSpec {

  public static final ArgSpec NONE = builder().build();

  // arg name -> allowed values (empty set means "any value allowed").
  private final Map<String, Set<String>> valueDomains;

  private ArgSpec(Map<String, Set<String>> valueDomains) {
    this.valueDomains = valueDomains;
  }

  public Set<String> allowedArgs() {
    return valueDomains.keySet();
  }

  public boolean allows(String arg) {
    return valueDomains.containsKey(arg);
  }

  /** Validate an accepted arg's value against its domain (no-op if the domain is empty). */
  public void validateValue(String endpoint, String arg, String value) {
    Set<String> domain = valueDomains.get(arg);
    if (domain == null || domain.isEmpty()) {
      return;
    }
    if (value == null || !domain.contains(value.toLowerCase(Locale.ROOT))) {
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

    public Builder arg(String name) {
      valueDomains.put(name, Set.of());
      return this;
    }

    public Builder arg(String name, Set<String> domain) {
      valueDomains.put(name, Set.copyOf(domain));
      return this;
    }

    public ArgSpec build() {
      return new ArgSpec(new LinkedHashMap<>(valueDomains));
    }
  }
}
