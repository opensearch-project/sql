/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import java.util.EnumMap;
import java.util.Map;
import org.opensearch.sql.spi.rest.RedactionClass;
import org.opensearch.sql.spi.rest.Redactor;

/**
 * The platform-owned map of {@link RedactionClass} to {@link Redactor} that the {@code rest} choke
 * point consults when shaping rows. This is deliberately NOT a {@code loadExtensions} SPI:
 * redaction is a security control, so an open extension point would let any installed plugin
 * register a no-op masker and weaken masking. Instead the registry is populated only by the
 * platform through {@link #register} or the constructor.
 *
 * <p>OSS leaves it EMPTY, so masking is a pure no-op out of the box. A managed distribution
 * supplies per-class {@link Redactor}s through a wrapper patch (precedent: the AOS SQL patch),
 * reusing one masker for every endpoint that declares a column of that class. {@link
 * RedactionClass#NONE} is a sentinel meaning "never redact" and cannot be registered.
 */
public final class RedactionRegistry {

  private final Map<RedactionClass, Redactor> redactors;

  /** An empty registry: every class is a no-op passthrough (the OSS default). */
  public RedactionRegistry() {
    this.redactors = new EnumMap<>(RedactionClass.class);
  }

  /** A registry pre-populated from a class -> masker map (the managed wrapper seam). */
  public RedactionRegistry(Map<RedactionClass, Redactor> redactors) {
    this();
    if (redactors != null) {
      redactors.forEach(this::register);
    }
  }

  /**
   * Register the masker for one sensitivity class. Rejects {@link RedactionClass#NONE} (the
   * never-redact sentinel) and null arguments so the registry stays auditable.
   */
  public void register(RedactionClass redactionClass, Redactor redactor) {
    if (redactionClass == null || redactionClass == RedactionClass.NONE) {
      throw new IllegalArgumentException("cannot register a redactor for redaction class: NONE");
    }
    if (redactor == null) {
      throw new IllegalArgumentException(
          "redactor for redaction class [" + redactionClass + "] must not be null");
    }
    redactors.put(redactionClass, redactor);
  }

  /** True when no masker is registered (the OSS default): every cell passes through unchanged. */
  public boolean isEmpty() {
    return redactors.isEmpty();
  }

  /**
   * Mask one cell value for its column's class. A {@code null}/{@code NONE} class, a null value, or
   * a class with no registered masker all pass the value through unchanged.
   */
  public String mask(RedactionClass redactionClass, String value) {
    if (redactionClass == null || redactionClass == RedactionClass.NONE || value == null) {
      return value;
    }
    Redactor redactor = redactors.get(redactionClass);
    return redactor == null ? value : redactor.mask(value);
  }
}
