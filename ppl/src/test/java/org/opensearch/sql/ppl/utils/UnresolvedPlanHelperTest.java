/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Collections;
import junit.framework.TestCase;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

@RunWith(MockitoJUnitRunner.class)
public class UnresolvedPlanHelperTest extends TestCase {

  @Test
  public void addProjectForRenameOperator() {
    Rename rename = Mockito.mock(Rename.class);

    UnresolvedPlan plan = UnresolvedPlanHelper.addSelectAll(rename);
    assertTrue(plan instanceof Project);
  }

  @Test
  public void addProjectForProjectExcludeOperator() {
    Project project = Mockito.mock(Project.class);
    when(project.isExcluded()).thenReturn(true);

    UnresolvedPlan plan = UnresolvedPlanHelper.addSelectAll(project);
    assertTrue(plan instanceof Project);
    assertThat(((Project) plan).getProjectList(), Matchers.contains(AllFields.of()));
  }

  @Test
  public void dontAddProjectForProjectOperator() {
    Project project = Mockito.mock(Project.class);
    UnresolvedExpression expression = Mockito.mock(UnresolvedExpression.class);
    when(project.isExcluded()).thenReturn(false);
    when(project.getProjectList()).thenReturn(Collections.singletonList(expression));

    UnresolvedPlan plan = UnresolvedPlanHelper.addSelectAll(project);
    assertTrue(plan instanceof Project);
    assertThat(((Project) plan).getProjectList(), Matchers.contains(expression));
  }
}
