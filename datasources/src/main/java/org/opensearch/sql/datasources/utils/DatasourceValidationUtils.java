package org.opensearch.sql.datasources.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.common.utils.URIValidationUtils;

/** Common Validation methods for all datasource connectors. */
@UtilityClass
public class DatasourceValidationUtils {

  public static final int MAX_LENGTH_FOR_CONFIG_PROPERTY = 1000;

  public static void validateHost(String uriString, List<String> denyHostList)
      throws URISyntaxException, UnknownHostException {
    if (!URIValidationUtils.validateURIHost(new URI(uriString).getHost(), denyHostList)) {
      throw new IllegalArgumentException(
          "Disallowed hostname in the uri. "
              + "Validate with plugins.query.datasources.uri.hosts.denylist config");
    }
    ;
  }

  public static void validateLengthAndRequiredFields(
      Map<String, String> config, Set<String> fields) {
    Set<String> missingFields = new HashSet<>();
    Set<String> invalidLengthFields = new HashSet<>();
    for (String field : fields) {
      if (!config.containsKey(field)) {
        missingFields.add(field);
      } else if (config.get(field).length() > MAX_LENGTH_FOR_CONFIG_PROPERTY) {
        invalidLengthFields.add(field);
      }
    }
    StringBuilder errorStringBuilder = new StringBuilder();
    if (missingFields.size() > 0) {
      errorStringBuilder.append(
          String.format(
              "Missing %s fields in the Prometheus connector properties.", missingFields));
    }

    if (invalidLengthFields.size() > 0) {
      errorStringBuilder.append(
          String.format("Fields %s exceeds more than 1000 characters.", invalidLengthFields));
    }
    if (errorStringBuilder.length() > 0) {
      throw new IllegalArgumentException(errorStringBuilder.toString());
    }
  }
}
