/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.util.List;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.junit.Test;

/**
 * Reading a field of a ROW column. An engine that stores an OpenSearch {@code object} as typed
 * columns declares it as a ROW, unlike the v2 path which declares MAP because it only passes {@code
 * _source} JSON through.
 */
public class CalcitePPLStructFieldTest extends CalcitePPLAbstractTest {

  public CalcitePPLStructFieldTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Override
  protected Frameworks.ConfigBuilder config(CalciteAssert.SchemaSpec... schemaSpecs) {
    SchemaPlus root = Frameworks.createRootSchema(true);
    SchemaPlus schema = root.add("structs", new ReflectiveSchema(new Docs()));
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /** One segment left over: field access on the ROW, not an ITEM lookup. */
  @Test
  public void testLeafOfAStruct() {
    RelNode root = getRelNode("source=docs | fields city.name");
    verifyLogical(
        root,
        "" + "LogicalProject($f0=[$1.name])\n" + "  LogicalTableScan(table=[[structs, docs]])\n");
  }

  /** Two segments: one access per level, which a single joined ITEM key cannot express. */
  @Test
  public void testLeafTwoLevelsDown() {
    RelNode root = getRelNode("source=docs | fields city.geo.lat");
    verifyLogical(
        root,
        ""
            + "LogicalProject($f0=[$1.geo.lat])\n"
            + "  LogicalTableScan(table=[[structs, docs]])\n");
  }

  /** The object itself stays one column. */
  @Test
  public void testWholeStruct() {
    RelNode root = getRelNode("source=docs | fields city");
    verifyLogical(
        root, "" + "LogicalProject(city=[$1])\n" + "  LogicalTableScan(table=[[structs, docs]])\n");
  }

  /** Test fixtures: a document with a nested object, which ReflectiveSchema declares as a ROW. */
  public static class Docs {
    public final Doc[] docs = {new Doc("s1", new City("seattle", new Geo(47.6, -122.3)))};
  }

  public static class Doc {
    public final String id;
    public final City city;

    public Doc(String id, City city) {
      this.id = id;
      this.city = city;
    }
  }

  public static class City {
    public final String name;
    public final Geo geo;

    public City(String name, Geo geo) {
      this.name = name;
      this.geo = geo;
    }
  }

  public static class Geo {
    public final double lat;
    public final double lon;

    public Geo(double lat, double lon) {
      this.lat = lat;
      this.lon = lon;
    }
  }
}
