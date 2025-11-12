/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.AbstractNodeVisitor;

public class SearchTest {

  private UnresolvedPlan mockChild;
  private Search search;
  private String testQueryString;

  @BeforeEach
  public void setUp() {
    mockChild = mock(UnresolvedPlan.class);
    testQueryString = "field1:value1 AND field2:value2";
    search = new Search(mockChild, testQueryString, null);
  }

  @Test
  public void testConstructor() {
    assertNotNull(search);
    assertEquals(mockChild, search.getChild().get(0));
    assertEquals(testQueryString, search.getQueryString());
  }

  @Test
  public void testGetChild() {
    List<UnresolvedPlan> children = search.getChild();
    assertNotNull(children);
    assertEquals(1, children.size());
    assertEquals(mockChild, children.get(0));
    assertTrue(children instanceof ImmutableList);
  }

  @Test
  public void testGetQueryString() {
    assertEquals(testQueryString, search.getQueryString());
  }

  @Test
  public void testAccept() {
    AbstractNodeVisitor<String, Object> mockVisitor = mock(AbstractNodeVisitor.class);
    Object mockContext = new Object();
    when(mockVisitor.visitSearch(search, mockContext)).thenReturn("visited");

    String result = search.accept(mockVisitor, mockContext);

    assertEquals("visited", result);
    verify(mockVisitor).visitSearch(search, mockContext);
  }

  @Test
  public void testEquals() {
    UnresolvedPlan sameChild = mockChild;
    String sameQueryString = testQueryString;
    Search sameSearch = new Search(sameChild, sameQueryString, null);

    assertEquals(search, sameSearch);
    assertEquals(search, search);
  }

  @Test
  public void testNotEqualsWithDifferentChild() {
    UnresolvedPlan differentChild = mock(UnresolvedPlan.class);
    Search differentSearch = new Search(differentChild, testQueryString, null);

    assertNotEquals(search, differentSearch);
  }

  @Test
  public void testNotEqualsWithDifferentQueryString() {
    Search differentSearch = new Search(mockChild, "different:query", null);

    assertNotEquals(search, differentSearch);
  }

  @Test
  public void testNotEqualsWithNull() {
    assertNotEquals(search, null);
  }

  @Test
  public void testNotEqualsWithDifferentClass() {
    assertNotEquals(search, "not a Search object");
  }

  @Test
  public void testHashCode() {
    Search sameSearch = new Search(mockChild, testQueryString, null);
    assertEquals(search.hashCode(), sameSearch.hashCode());
  }

  @Test
  public void testHashCodeWithDifferentValues() {
    UnresolvedPlan differentChild = mock(UnresolvedPlan.class);
    Search differentSearch = new Search(differentChild, "different:query", null);
    assertNotEquals(search.hashCode(), differentSearch.hashCode());
  }

  @Test
  public void testToString() {
    String toStringResult = search.toString();
    assertNotNull(toStringResult);
    assertTrue(toStringResult.contains("Search"));
    assertTrue(toStringResult.contains("queryString=" + testQueryString));
  }

  @Test
  public void testWithEmptyQueryString() {
    Search emptySearch = new Search(mockChild, "", null);
    assertEquals("", emptySearch.getQueryString());
    assertEquals(mockChild, emptySearch.getChild().get(0));
  }

  @Test
  public void testWithComplexQueryString() {
    String complexQuery = "(field1:value1 OR field2:value2) AND NOT field3:value3";
    Search complexSearch = new Search(mockChild, complexQuery, null);
    assertEquals(complexQuery, complexSearch.getQueryString());
  }

  @Test
  public void testWithSpecialCharactersInQueryString() {
    String specialCharsQuery = "field:\"value with spaces\" AND field2:value*";
    Search specialSearch = new Search(mockChild, specialCharsQuery, null);
    assertEquals(specialCharsQuery, specialSearch.getQueryString());
  }
}
