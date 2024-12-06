package org.opensearch.sql.datasources.utils;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

import java.util.HashMap;
import java.util.Set;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DatasourceValidationUtilsTest {

  @SneakyThrows
  @Test
  public void testValidateHost() {
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                DatasourceValidationUtils.validateHost(
                    "http://localhost:9090", singletonList("127.0.0.0/8")));
    Assertions.assertEquals(
        "Disallowed hostname in the uri. Validate with plugins.query.datasources.uri.hosts.denylist"
            + " config",
        illegalArgumentException.getMessage());
  }

  @SneakyThrows
  @Test
  public void testValidateHostWithSuccess() {
    Assertions.assertDoesNotThrow(
        () ->
            DatasourceValidationUtils.validateHost(
                "http://localhost:9090", singletonList("192.168.0.0/8")));
  }

  @Test
  public void testValidateLengthAndRequiredFieldsWithAbsentField() {
    HashMap<String, String> config = new HashMap<>();
    config.put("s3.uri", "test");
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                DatasourceValidationUtils.validateLengthAndRequiredFields(
                    config, Set.of("s3.uri", "s3.auth.type")));
    Assertions.assertEquals(
        "Missing [s3.auth.type] fields in the connector properties.",
        illegalArgumentException.getMessage());
  }

  @Test
  public void testValidateLengthAndRequiredFieldsWithInvalidLength() {
    HashMap<String, String> config = new HashMap<>();
    config.put("s3.uri", RandomStringUtils.random(1001));
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                DatasourceValidationUtils.validateLengthAndRequiredFields(
                    config, Set.of("s3.uri", "s3.auth.type")));
    Assertions.assertEquals(
        "Missing [s3.auth.type] fields in the connector properties.Fields "
            + "[s3.uri] exceeds more than 1000 characters.",
        illegalArgumentException.getMessage());
  }

  @Test
  public void testValidateLengthAndRequiredFieldsWithSuccess() {
    HashMap<String, String> config = new HashMap<>();
    config.put("s3.uri", "test");
    Assertions.assertDoesNotThrow(
        () -> DatasourceValidationUtils.validateLengthAndRequiredFields(config, emptySet()));
  }
}
