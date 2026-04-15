/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec;

import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.opensearch.sql.api.spec.datetime.DatetimeUdtExtension;

/**
 * PPL language specification.
 *
 * <p>Note: PPL currently has its own parsing and analyzing pipeline, so only configuration and
 * extensions applied after RelNode construction are in use. The parser and validator configs
 * returned here are inert for the PPL path.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UnifiedPplSpec implements LanguageSpec {

  public static UnifiedPplSpec create() {
    return new UnifiedPplSpec();
  }

  @Override
  public SqlParser.Config parserConfig() {
    return SqlParser.config();
  }

  @Override
  public SqlValidator.Config validatorConfig() {
    return SqlValidator.Config.DEFAULT;
  }

  @Override
  public List<LanguageExtension> extensions() {
    return List.of(new DatetimeUdtExtension());
  }
}
