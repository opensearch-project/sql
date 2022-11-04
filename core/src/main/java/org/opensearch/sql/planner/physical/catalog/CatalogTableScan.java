/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.catalog;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.catalog.model.Catalog;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * This class handles table scan of catalog table.
 * Right now these are derived from catalogService thorough static fields.
 * In future this might scan data from underlying datastore if we start
 * persisting catalog info somewhere.
 *
 */
public class CatalogTableScan extends TableScanOperator {

  private final CatalogService catalogService;

  private Iterator<ExprValue> iterator;

  public CatalogTableScan(CatalogService catalogService) {
    this.catalogService = catalogService;
    this.iterator = Collections.emptyIterator();
  }

  @Override
  public String explain() {
    return "GetCatalogRequestRequest{}";
  }

  @Override
  public void open() {
    List<ExprValue> exprValues = new ArrayList<>();
    Set<Catalog> catalogs = catalogService.getCatalogs();
    for (Catalog catalog : catalogs) {
      exprValues.add(
          new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
              "DATASOURCE_NAME", ExprValueUtils.stringValue(catalog.getName()),
              "CONNECTOR_TYPE", ExprValueUtils.stringValue(catalog.getConnectorType().name())))));
    }
    iterator = exprValues.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

}
