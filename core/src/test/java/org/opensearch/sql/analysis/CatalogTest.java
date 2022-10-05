/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.analysis;

import java.util.HashSet;
import java.util.Set;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

public class CatalogTest {



  static class CatalogResolver {
    public IdName resolve(String tableName) {
      Table table = new Table();
      Catalog catalog = new Catalog();
      Schema schema = new Schema();

      String left = table.capture(schema.capture(catalog.capture(tableName)));
      assert StringUtils.isEmpty(left);
      return new IdName(catalog.getCatalog(), schema.getSchema(), table.getTable());
    }
  }

  @Data
  static class IdName {
    private final String catalog;
    private final String schema;
    private final String table;
  }

  static class Catalog {
    private final String DEFAULT = "opensearch";
    private final Set<String> catalogs = new HashSet<>();

    @Getter
    private String catalog;

    public Catalog() {
      this.catalogs.add("s3");
      this.catalogs.add("prometheus");
    }

    String capture(String name) {
      int i = name.indexOf('.');
      if (i == -1) {
        catalog = DEFAULT;
        return name;
      } else if (!catalogs.contains(name.substring(0, i))) {
        catalog = DEFAULT;
        return name;
      } else {
        catalog = name.substring(0, i);
        return name.substring(i);
      }
    }
  }

  static class Schema {
    private final String DEFAULT = "default";
    private final String INFORMATION = "information_schema";

    @Getter
    private String schema;

    String capture(String name) {
      int i = name.indexOf('.');
      if (i == -1) {
        schema = DEFAULT;
        return name;
      }
      String possibleSchema = name.substring(0, i);

      if (DEFAULT.equals(possibleSchema) || INFORMATION.equals(possibleSchema)) {
        schema = possibleSchema;
        return name.substring(i);
      } else {
        return name;
      }
    }
  }

  static class Table {
    @Getter
    private String table;

    String capture(String name) {
      table = name;
      return "";
    }
  }
}
