/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
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
    schema.add("maps", new TableWithMap());
    schema.add("structmaps", new TableWithStructAndMap());
    return Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.Config.DEFAULT)
        .defaultSchema(schema)
        .traitDefs((List<RelTraitDef>) null)
        .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2));
  }

  /**
   * One segment left over: field access on the ROW, not an ITEM lookup.
   *
   * <p>The projected column keeps the dotted name. A field access carries no name of its own, so
   * without the alias Calcite derives {@code $f0} and the response schema no longer reports the
   * field the user asked for.
   */
  @Test
  public void testLeafOfAStruct() {
    RelNode root = getRelNode("source=docs | fields city.name");
    verifyLogical(
        root,
        ""
            + "LogicalProject(city.name=[$1.name])\n"
            + "  LogicalTableScan(table=[[structs, docs]])\n");
  }

  /** Two segments: one access per level, which a single joined ITEM key cannot express. */
  @Test
  public void testLeafTwoLevelsDown() {
    RelNode root = getRelNode("source=docs | fields city.geo.lat");
    verifyLogical(
        root,
        ""
            + "LogicalProject(city.geo.lat=[$1.geo.lat])\n"
            + "  LogicalTableScan(table=[[structs, docs]])\n");
  }

  /** The object itself stays one column. */
  @Test
  public void testWholeStruct() {
    RelNode root = getRelNode("source=docs | fields city");
    verifyLogical(
        root, "" + "LogicalProject(city=[$1])\n" + "  LogicalTableScan(table=[[structs, docs]])\n");
  }

  /** A struct path works in a predicate, not only in a projection. */
  @Test
  public void testLeafOfAStructInAPredicate() {
    RelNode root = getRelNode("source=docs | where city.geo.lat > 40 | fields id");
    verifyLogical(
        root,
        ""
            + "LogicalProject(id=[$0])\n"
            + "  LogicalFilter(condition=[>($1.geo.lat, 40)])\n"
            + "    LogicalTableScan(table=[[structs, docs]])\n");
  }

  /**
   * A backtick-quoted path is a single part, so the prefix walk has nothing to descend and matches
   * only a column literally named {@code city.geo.lat}. Splitting it and retrying reaches the same
   * field access as the unquoted form.
   */
  @Test
  public void testQuotedDottedPathIsSplitAndRetried() {
    RelNode root = getRelNode("source=docs | fields `city.geo.lat`");
    verifyLogical(
        root,
        ""
            + "LogicalProject(city.geo.lat=[$1.geo.lat])\n"
            + "  LogicalTableScan(table=[[structs, docs]])\n");
  }

  /**
   * A segment naming no field of the ROW is an ordinary not-found, not a 500.
   *
   * <p>Descent stops as soon as a segment misses, and the remainder must not become an {@code ITEM}
   * key on the ROW: {@code ITEM(<ROW>, 'nonexistent')} is the shape that makes {@code
   * SqlItemOperator} throw {@code AssertionError: Cannot infer type of field ... within ROW type},
   * and as an {@code Error} it escapes the {@code catch (Exception)} around resolution and reaches
   * the user as a 500. Reporting the miss as unresolved lets it fall through to the ordinary {@code
   * Field [...] not found} with its available-field suggestions.
   */
  @Test
  public void testUndeclaredFieldOfAStructIsNotFound() {
    Throwable thrown =
        assertThrows(Throwable.class, () -> getRelNode("source=docs | fields city.nonexistent"));
    assertFalse(
        "an undeclared ROW field must not surface as an AssertionError, got: " + thrown,
        thrown instanceof AssertionError);
    assertTrue(
        "expected a not-found naming the field, got: " + thrown,
        String.valueOf(thrown.getMessage()).contains("city.nonexistent"));
  }

  /**
   * An unquoted dotted path over a MAP still becomes one ITEM key, which is what the v2 path needs
   * since it stores objects flattened.
   */
  @Test
  public void testDottedPathOverAMapIsStillOneItemKey() {
    RelNode root = getRelNode("source=maps | fields data.custom");
    verifyLogical(
        root,
        ""
            + "LogicalProject(data.custom=[ITEM($1, 'custom')])\n"
            + "  LogicalTableScan(table=[[structs, maps]])\n");
  }

  /**
   * A quoted dotted name over a MAP must stay unresolved rather than being split into a map key.
   *
   * <p>Splitting is for reaching into a ROW, where a dotted name is a path. Over a MAP the literal
   * lookup is already the right question, and code above relies on the negative answer: when a
   * container column is rebuilt its dotted subtree is shed, so a `data.custom` created in between
   * is meant to be gone. Splitting it back into {@code ITEM(data, 'custom')} would resolve it again
   * and quietly return null instead of reporting the field as missing.
   */
  @Test
  public void testQuotedDottedNameOverAMapIsNotSplit() {
    Throwable thrown =
        assertThrows(Throwable.class, () -> getRelNode("source=maps | fields `data.custom`"));
    assertTrue(
        "expected a not-found for the quoted name, got: " + thrown,
        String.valueOf(thrown.getMessage()).contains("data.custom"));
  }

  /**
   * A path that continues past the end of the object is a not-found, not a 500.
   *
   * <p>{@code city.name} is a VARCHAR, so descent stops having left the ROW behind. Rejecting the
   * leftover only while still standing on a ROW let this fall through to {@code ITEM(VARCHAR,
   * 'bogus')}, and {@code SqlItemOperator} accepts only ARRAY / MAP / ROW / ANY / VARIANT --
   * anything else throws a bare {@code AssertionError}, which escapes {@code catch (Exception)} as
   * a 500.
   */
  @Test
  public void testLeftoverPathAfterLeavingTheStructIsNotFound() {
    Throwable thrown =
        assertThrows(Throwable.class, () -> getRelNode("source=docs | fields city.name.bogus"));
    assertFalse(
        "a path running past a scalar must not surface as an AssertionError, got: " + thrown,
        thrown instanceof AssertionError);
    assertTrue(
        "expected a not-found naming the field, got: " + thrown,
        String.valueOf(thrown.getMessage()).contains("city.name.bogus"));
  }

  /**
   * The guard above is deliberately narrower than "reject any leftover once a ROW was entered".
   *
   * <p>A {@code flat_object} inside an {@code object} is a MAP child of the struct, so the path
   * legitimately descends the ROW and then keys into the map. The rule is "reject a leftover the
   * type cannot address", not "reject any leftover".
   */
  @Test
  public void testMapInsideAStructStillTakesTheRemainderAsAnItemKey() {
    RelNode root = getRelNode("source=structmaps | fields city.meta.region");
    verifyLogical(
        root,
        ""
            + "LogicalProject(city.meta.region=[ITEM($1.meta, 'region')])\n"
            + "  LogicalTableScan(table=[[structs, structmaps]])\n");
  }

  /**
   * A failed descent reached through a table alias is a not-found, not an NPE.
   *
   * <p>{@code resolveFieldAccess} returns null for a path that stops inside a ROW. The alias branch
   * wrapped its result in {@code Optional.of} regardless, so a qualified miss threw. The alias
   * branch runs before the unqualified walk, so it is the one a qualified path hits first.
   */
  @Test
  public void testFailedDescentThroughTheAliasPathIsNotFound() {
    Throwable thrown =
        assertThrows(Throwable.class, () -> getRelNode("source=docs as d | fields d.city.nope"));
    assertFalse(
        "a failed descent through an alias must not surface as an NPE, got: " + thrown,
        thrown instanceof NullPointerException);
    assertTrue(
        "expected a not-found naming the field, got: " + thrown,
        String.valueOf(thrown.getMessage()).contains("city.nope"));
  }

  /** A struct with a MAP child, which is how a {@code flat_object} inside an object is declared. */
  private static class TableWithStructAndMap extends AbstractTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      RelDataType city =
          typeFactory
              .builder()
              .add("name", varchar)
              .add(
                  "meta",
                  typeFactory.createTypeWithNullability(
                      typeFactory.createMapType(varchar, varchar), true))
              .build();
      return typeFactory
          .builder()
          .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
          .add("city", typeFactory.createTypeWithNullability(city, true))
          .build();
    }
  }

  /** A table with a MAP column, the shape the ReflectiveSchema fixture above cannot express. */
  private static class TableWithMap extends AbstractTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
      return typeFactory
          .builder()
          .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
          .add(
              "data",
              typeFactory.createTypeWithNullability(
                  typeFactory.createMapType(varchar, varchar), true))
          .build();
    }
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
