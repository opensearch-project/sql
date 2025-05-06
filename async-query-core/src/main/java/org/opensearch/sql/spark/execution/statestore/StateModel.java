/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statestore;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import lombok.Builder.Default;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@SuperBuilder
public abstract class StateModel {
  @Getter @EqualsAndHashCode.Exclude @Default
  private final ImmutableMap<String, Object> metadata = ImmutableMap.of();

  public abstract String getId();

  public <T> Optional<T> getMetadataItem(String name, Class<T> type) {
    if (metadata.containsKey(name)) {
      Object value = metadata.get(name);
      if (type.isInstance(value)) {
        return Optional.of(type.cast(value));
      } else {
        throw new RuntimeException(
            String.format(
                "The metadata field %s is an instance of %s instead of %s",
                name, value.getClass(), type));
      }
    } else {
      return Optional.empty();
    }
  }
}
