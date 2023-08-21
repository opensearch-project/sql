/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.antlr.semantic.scope;

import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.BOOLEAN;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.DATE;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.KEYWORD;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.OBJECT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchDataType.TEXT;
import static org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex.IndexType.NESTED_FIELD;

import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.legacy.antlr.semantic.types.Type;
import org.opensearch.sql.legacy.antlr.semantic.types.base.OpenSearchIndex;

/** Test cases for environment */
public class EnvironmentTest {

  /** Use context class for push/pop */
  private final SemanticContext context = new SemanticContext();

  @Test
  public void defineFieldSymbolInDifferentEnvironmentsShouldBeAbleToResolve() {
    // Root environment
    Symbol birthday = new Symbol(Namespace.FIELD_NAME, "s.birthday");
    environment().define(birthday, DATE);
    Assert.assertTrue(environment().resolve(birthday).isPresent());

    // New environment 1
    context.push();
    Symbol city = new Symbol(Namespace.FIELD_NAME, "s.city");
    environment().define(city, KEYWORD);
    Assert.assertTrue(environment().resolve(birthday).isPresent());
    Assert.assertTrue(environment().resolve(city).isPresent());

    // New environment 2
    context.push();
    Symbol manager = new Symbol(Namespace.FIELD_NAME, "s.manager");
    environment().define(manager, OBJECT);
    Assert.assertTrue(environment().resolve(birthday).isPresent());
    Assert.assertTrue(environment().resolve(city).isPresent());
    Assert.assertTrue(environment().resolve(manager).isPresent());
  }

  @Test
  public void defineFieldSymbolInDifferentEnvironmentsShouldNotAbleToResolveOncePopped() {
    // Root environment
    Symbol birthday = new Symbol(Namespace.FIELD_NAME, "s.birthday");
    environment().define(birthday, DATE);

    // New environment
    context.push();
    Symbol city = new Symbol(Namespace.FIELD_NAME, "s.city");
    Symbol manager = new Symbol(Namespace.FIELD_NAME, "s.manager");
    environment().define(city, OBJECT);
    environment().define(manager, OBJECT);
    Assert.assertTrue(environment().resolve(birthday).isPresent());
    Assert.assertTrue(environment().resolve(city).isPresent());
    Assert.assertTrue(environment().resolve(manager).isPresent());

    context.pop();
    Assert.assertFalse(environment().resolve(city).isPresent());
    Assert.assertFalse(environment().resolve(manager).isPresent());
    Assert.assertTrue(environment().resolve(birthday).isPresent());
  }

  @Test
  public void defineFieldSymbolInDifferentEnvironmentsShouldBeAbleToResolveByPrefix() {
    // Root environment
    Symbol birthday = new Symbol(Namespace.FIELD_NAME, "s.birthday");
    environment().define(birthday, DATE);

    // New environment 1
    context.push();
    Symbol city = new Symbol(Namespace.FIELD_NAME, "s.city");
    environment().define(city, KEYWORD);

    // New environment 2
    context.push();
    Symbol manager = new Symbol(Namespace.FIELD_NAME, "s.manager");
    environment().define(manager, OBJECT);

    Map<String, Type> typeByName =
        environment().resolveByPrefix(new Symbol(Namespace.FIELD_NAME, "s"));
    assertThat(
        typeByName,
        allOf(
            aMapWithSize(3),
            hasEntry("s.birthday", DATE),
            hasEntry("s.city", KEYWORD),
            hasEntry("s.manager", OBJECT)));
  }

  @Test
  public void defineFieldSymbolShouldBeAbleToResolveAll() {
    environment()
        .define(
            new Symbol(Namespace.FIELD_NAME, "s.projects"),
            new OpenSearchIndex("s.projects", NESTED_FIELD));
    environment().define(new Symbol(Namespace.FIELD_NAME, "s.projects.release"), DATE);
    environment().define(new Symbol(Namespace.FIELD_NAME, "s.projects.active"), BOOLEAN);
    environment().define(new Symbol(Namespace.FIELD_NAME, "s.address"), TEXT);
    environment().define(new Symbol(Namespace.FIELD_NAME, "s.city"), KEYWORD);
    environment().define(new Symbol(Namespace.FIELD_NAME, "s.manager.name"), TEXT);

    Map<String, Type> typeByName = environment().resolveAll(Namespace.FIELD_NAME);
    assertThat(
        typeByName,
        allOf(
            aMapWithSize(6),
            hasEntry("s.projects", (Type) new OpenSearchIndex("s.projects", NESTED_FIELD)),
            hasEntry("s.projects.release", DATE),
            hasEntry("s.projects.active", BOOLEAN),
            hasEntry("s.address", TEXT),
            hasEntry("s.city", KEYWORD),
            hasEntry("s.manager.name", TEXT)));
  }

  @Test
  public void defineFieldSymbolInDifferentEnvironmentsShouldBeAbleToResolveAll() {
    // Root environment
    Symbol birthday = new Symbol(Namespace.FIELD_NAME, "s.birthday");
    environment().define(birthday, DATE);

    // New environment 1
    context.push();
    Symbol city = new Symbol(Namespace.FIELD_NAME, "s.city");
    environment().define(city, KEYWORD);

    // New environment 2
    context.push();
    Symbol manager = new Symbol(Namespace.FIELD_NAME, "s.manager");
    environment().define(manager, OBJECT);

    Map<String, Type> typeByName = environment().resolveAll(Namespace.FIELD_NAME);
    assertThat(
        typeByName,
        allOf(
            aMapWithSize(3),
            hasEntry("s.birthday", DATE),
            hasEntry("s.city", KEYWORD),
            hasEntry("s.manager", OBJECT)));
  }

  private Environment environment() {
    return context.peek();
  }
}
