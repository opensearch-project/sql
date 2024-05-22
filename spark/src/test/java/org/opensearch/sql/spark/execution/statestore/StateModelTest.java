package org.opensearch.sql.spark.execution.statestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import lombok.Data;
import lombok.experimental.SuperBuilder;
import org.junit.jupiter.api.Test;

class StateModelTest {

  public static final String METADATA_KEY = "KEY";
  public static final String METADATA_VALUE = "VALUE";
  public static final String UNKNOWN_KEY = "UNKNOWN_KEY";

  @Data
  @SuperBuilder
  static class ConcreteStateModel extends StateModel {
    @Override
    public String getId() {
      return null;
    }
  }

  ConcreteStateModel model =
      ConcreteStateModel.builder().metadata(ImmutableMap.of(METADATA_KEY, METADATA_VALUE)).build();

  @Test
  public void whenMetadataExist() {
    Optional<String> result = model.getMetadataItem(METADATA_KEY, String.class);

    assertEquals(METADATA_VALUE, result.get());
  }

  @Test
  public void whenMetadataNotExist() {
    Optional<String> result = model.getMetadataItem(UNKNOWN_KEY, String.class);

    assertFalse(result.isPresent());
  }

  @Test
  public void whenTypeDoNotMatch() {
    assertThrows(RuntimeException.class, () -> model.getMetadataItem(METADATA_KEY, Long.class));
  }
}
