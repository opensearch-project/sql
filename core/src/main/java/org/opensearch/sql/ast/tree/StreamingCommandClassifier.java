/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Set;

/**
 * Centralized classifier for determining whether PPL commands are streaming or non-streaming.
 *
 * <p>Streaming commands process events individually without needing to see other events, enabling
 * parallel execution and result interleaving in multisearch.
 *
 * <p>Non-streaming commands require the complete dataset before producing output, such as
 * aggregation (stats) or sorting operations.
 */
public class StreamingCommandClassifier {

  /**
   * Set of command classes that are known to be streaming. These commands process each event
   * independently.
   */
  private static final Set<Class<? extends UnresolvedPlan>> STREAMING_COMMANDS =
      Set.of(
          // Basic data operations
          Eval.class, // Transforms fields for each event independently
          Filter.class, // Evaluates each event against criteria independently
          Project.class, // Selects fields from each event independently
          Relation.class, // Relation is the base data source operation
          Search.class, // Search filters events using query_string expressions

          // Result limiting and ordering (streaming variants)
          Head.class, // Takes first N events from stream
          Limit.class, // Processes first N events from stream
          Reverse.class, // Outputs events in reverse order without grouping

          // Field manipulation
          Rename.class, // Transforms field names for each event independently
          Rex.class, // Extracts fields from each event using regex patterns
          Regex.class, // Filters each event based on pattern matching
          Parse.class, // Extracts fields from each event independently

          // Data transformation
          Expand.class, // Converts array fields to separate events
          Flatten.class, // Unnests nested fields for each event
          FillNull.class, // Replaces null values in each event independently

          // Meta commands
          Multisearch.class // Multisearch is a generating command (generates events)
          );

  /**
   * Set of command classes that are known to be non-streaming. These commands require all events
   * before producing output.
   */
  private static final Set<Class<? extends UnresolvedPlan>> NON_STREAMING_COMMANDS =
      Set.of(
          // Aggregation and statistics
          Aggregation.class, // Aggregation/stats requires all events for calculations

          // Sorting and ordering
          Sort.class, // Sort requires all events to determine order

          // Bucketing and grouping
          Bin.class, // Bin requires all events to calculate bucket ranges and group data
          Timechart.class, // Timechart aggregates data over time buckets requiring all events

          // Statistical analysis
          RareTopN.class, // Rare/Top requires all events to determine least/most common values
          Window.class, // Window functions require access to all events in the window
          Trendline.class, // Trendline calculation requires all events to compute trends

          // Data quality and deduplication
          Dedupe.class, // Dedupe requires all events to identify and remove duplicates

          // Joins and lookups
          Join.class, // Join requires all events from both datasets before matching
          Lookup.class // Lookup requires complete lookup table before enriching events
          );

  /**
   * Determines if a command is streaming (processes events individually).
   *
   * @param command The command to classify
   * @return true if the command is streaming, false if non-streaming
   */
  public static boolean isStreamingCommand(UnresolvedPlan command) {
    if (command == null) {
      return false;
    }

    Class<? extends UnresolvedPlan> commandClass = command.getClass();

    // Check explicit streaming commands
    if (STREAMING_COMMANDS.contains(commandClass)) {
      return true;
    }

    // Check explicit non-streaming commands
    if (NON_STREAMING_COMMANDS.contains(commandClass)) {
      return false;
    }

    // Conservative default - assume non-streaming for unknown commands
    // This ensures safety when new commands are added
    return false;
  }

  /**
   * Gets a user-friendly name for the command type.
   *
   * @param command The command to get the name for
   * @return A user-friendly command name
   */
  public static String getCommandName(UnresolvedPlan command) {
    if (command == null) {
      return "unknown";
    }

    String className = command.getClass().getSimpleName();

    // Convert common class names to PPL command names
    switch (className) {
      case "Aggregation":
        return "stats";
      case "Sort":
        return "sort";
      case "Filter":
        return "where";
      case "Project":
        return "fields";
      case "Eval":
        return "eval";
      case "Relation":
        return "relation";
      case "Search":
        return "search";
      case "Multisearch":
        return "multisearch";
      case "RareTopN":
        return "rare/top";
      default:
        // Convert CamelCase to lowercase for unknown commands
        return className.replaceAll("([A-Z])", "_$1").toLowerCase().substring(1);
    }
  }

  /**
   * Validates that all commands in a plan tree are streaming commands. Throws exception if any
   * non-streaming command is found.
   *
   * @param plan The plan to validate
   * @throws org.opensearch.sql.exception.SemanticCheckException if non-streaming commands are found
   */
  public static void validateStreamingCommands(UnresolvedPlan plan) {
    if (plan == null) {
      return;
    }

    // Check if current command is streaming
    if (!isStreamingCommand(plan)) {
      String commandName = getCommandName(plan);
      throw new org.opensearch.sql.exception.SemanticCheckException(
          "Non-streaming command '"
              + commandName
              + "' is not supported in multisearch subsearches. Commands like 'stats', 'sort', and"
              + " other aggregating operations require all events before producing output, which"
              + " conflicts with multisearch's event interleaving.");
    }

    // Recursively validate child commands
    if (plan.getChild() != null) {
      for (org.opensearch.sql.ast.Node childNode : plan.getChild()) {
        if (childNode instanceof UnresolvedPlan) {
          validateStreamingCommands((UnresolvedPlan) childNode);
        }
      }
    }
  }
}
