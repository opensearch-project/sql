/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.error;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/**
 * Error report that wraps exceptions and accumulates contextual information as errors bubble up
 * through system layers.
 *
 * <p>Inspired by Rust's anyhow/eyre libraries, this class allows each layer to add context without
 * modifying the original exception message.
 *
 * <p>Example usage:
 *
 * <pre>
 * try {
 *   resolveField(fieldName);
 * } catch (IllegalArgumentException e) {
 *   throw ErrorReport.wrap(e)
 *     .code(ErrorCode.FIELD_NOT_FOUND)
 *     .stage(QueryProcessingStage.ANALYZING)
 *     .location("while resolving fields in the index mapping")
 *     .suggestion("Did you mean: '" + suggestedField + "'?")
 *     .context("index_pattern", indexPattern)
 *     .context("position", cursorPosition)
 *     .build();
 * }
 * </pre>
 */
public class ErrorReport extends RuntimeException {

  @Getter private final Exception cause;
  @Getter private final ErrorCode code;
  @Getter private final QueryProcessingStage stage;
  private final List<String> locationChain;
  private final Map<String, Object> context;
  @Getter private final String suggestion;
  @Getter private final String details;

  private ErrorReport(Builder builder) {
    super(builder.cause.getMessage(), builder.cause);
    this.cause = builder.cause;
    this.code = builder.code;
    this.stage = builder.stage;
    this.locationChain = new ArrayList<>(builder.locationChain);
    this.context = new LinkedHashMap<>(builder.context);
    this.suggestion = builder.suggestion;
    this.details = builder.details;
  }

  /**
   * Wraps an exception with an error report builder. If the exception is already an ErrorReport,
   * returns a builder initialized with the existing report's data.
   *
   * @param cause The underlying exception
   * @return A builder for constructing the error report
   */
  public static Builder wrap(Exception cause) {
    if (cause instanceof ErrorReport existing) {
      return new Builder(existing.cause)
          .code(existing.code)
          .stage(existing.stage)
          .details(existing.details)
          .suggestion(existing.suggestion)
          .addLocationChain(existing.locationChain)
          .addContext(existing.context);
    }
    return new Builder(cause);
  }

  public List<String> getLocationChain() {
    return new ArrayList<>(locationChain);
  }

  public Map<String, Object> getContext() {
    return new LinkedHashMap<>(context);
  }

  /** Get the original exception type name. */
  public String getExceptionType() {
    return cause.getClass().getSimpleName();
  }

  /**
   * Format as a detailed message with all context information. This is suitable for logging or
   * detailed error displays.
   */
  public String toDetailedMessage() {
    StringBuilder sb = new StringBuilder();

    sb.append("Error");
    if (code != null && code != ErrorCode.UNKNOWN) {
      sb.append(" [").append(code).append("]");
    }
    if (stage != null) {
      sb.append(" at stage: ").append(stage.getDisplayName());
    }
    sb.append("\n");

    if (details != null) {
      sb.append("Details: ").append(details).append("\n");
    }

    if (!locationChain.isEmpty()) {
      sb.append("\nLocation chain:\n");
      for (int i = 0; i < locationChain.size(); i++) {
        // The location chain is typically appended to as we traverse up the stack, but for reading
        // the error it makes more sense to go down the stack. So we reverse it.
        sb.append("  ")
            .append(i + 1)
            .append(". ")
            .append(locationChain.get(locationChain.size() - i - 1))
            .append("\n");
      }
    }

    if (!context.isEmpty()) {
      sb.append("\nContext:\n");
      context.forEach(
          (key, value) -> sb.append("  ").append(key).append(": ").append(value).append("\n"));
    }

    if (suggestion != null) {
      sb.append("\nSuggestion: ").append(suggestion).append("\n");
    }

    return sb.toString();
  }

  /**
   * Convert to JSON-compatible map structure for REST API responses.
   *
   * @return Map containing error information in structured format
   */
  public Map<String, Object> toJsonMap() {
    Map<String, Object> json = new LinkedHashMap<>();

    json.put("type", getExceptionType());

    if (code != null) {
      json.put("code", code.name());
    }

    if (details != null) {
      json.put("details", details);
    }

    if (!locationChain.isEmpty()) {
      // The location chain is typically appended to as we traverse up the stack, but for reading
      // the error it makes more sense to go down the stack. So we reverse it.
      json.put("location", locationChain.reversed());
    }

    // Build context with stage information included
    Map<String, Object> contextMap = new LinkedHashMap<>(context);
    if (stage != null) {
      contextMap.put("stage", stage.toJsonKey());
      contextMap.put("stage_description", stage.getDisplayName());
    }
    if (!contextMap.isEmpty()) {
      json.put("context", contextMap);
    }

    if (suggestion != null) {
      json.put("suggestion", suggestion);
    }

    return json;
  }

  /** Builder for constructing error reports with contextual information. */
  public static class Builder {
    private final Exception cause;
    private ErrorCode code = ErrorCode.UNKNOWN;
    private QueryProcessingStage stage = null;
    private final List<String> locationChain = new ArrayList<>();
    private final Map<String, Object> context = new LinkedHashMap<>();
    private String suggestion = null;
    private String details = null;

    private Builder(Exception cause) {
      this.cause = cause;
      // Default details to the original exception message
      this.details = cause.getLocalizedMessage();
    }

    /** Set the machine-readable error code. */
    public Builder code(ErrorCode code) {
      this.code = code;
      return this;
    }

    /** Set the query processing stage where the error occurred. */
    public Builder stage(QueryProcessingStage stage) {
      this.stage = stage;
      return this;
    }

    /**
     * Add a location to the chain describing where the error occurred. Locations are added in order
     * from innermost to outermost layer.
     *
     * @param location Description like "while resolving fields in index mapping"
     */
    public Builder location(String location) {
      this.locationChain.add(location);
      return this;
    }

    /**
     * Add multiple locations from an existing chain.
     *
     * @param locations List of location descriptions
     */
    private Builder addLocationChain(List<String> locations) {
      this.locationChain.addAll(locations);
      return this;
    }

    /**
     * Add structured context data (index name, query, position, etc).
     *
     * @param key Context key
     * @param value Context value (will be converted to string for serialization)
     */
    public Builder context(String key, Object value) {
      this.context.put(key, value);
      return this;
    }

    /**
     * Add multiple context entries from an existing map.
     *
     * @param contextMap Map of context key-value pairs
     */
    private Builder addContext(Map<String, Object> contextMap) {
      this.context.putAll(contextMap);
      return this;
    }

    /**
     * Set a suggestion for how to fix the error.
     *
     * @param suggestion User-facing suggestion like "Did you mean: 'foo'?"
     */
    public Builder suggestion(String suggestion) {
      this.suggestion = suggestion;
      return this;
    }

    /**
     * Override the default details message. By default, uses the wrapped exception's message.
     *
     * @param details Custom details message
     */
    public Builder details(String details) {
      this.details = details;
      return this;
    }

    /**
     * Build and throw the error report as an exception.
     *
     * @return The constructed error report (can be thrown)
     */
    public ErrorReport build() {
      return new ErrorReport(this);
    }
  }
}
