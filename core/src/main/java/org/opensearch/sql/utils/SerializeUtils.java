/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.datasource.model.DataSourceType;

@UtilityClass
public class SerializeUtils {
  private static class DataSourceTypeSerializer implements JsonSerializer<DataSourceType> {
    @Override
    public JsonElement serialize(
        DataSourceType dataSourceType,
        Type type,
        JsonSerializationContext jsonSerializationContext) {
      return new JsonPrimitive(dataSourceType.name());
    }
  }

  private static class DataSourceTypeDeserializer implements JsonDeserializer<DataSourceType> {
    @Override
    public DataSourceType deserialize(
        JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext)
        throws JsonParseException {
      return DataSourceType.fromString(jsonElement.getAsString());
    }
  }

  public static GsonBuilder getGsonBuilder() {
    return new GsonBuilder()
        .registerTypeAdapter(DataSourceType.class, new DataSourceTypeSerializer())
        .registerTypeAdapter(DataSourceType.class, new DataSourceTypeDeserializer());
  }

  public static Gson buildGson() {
    return getGsonBuilder().create();
  }
}
