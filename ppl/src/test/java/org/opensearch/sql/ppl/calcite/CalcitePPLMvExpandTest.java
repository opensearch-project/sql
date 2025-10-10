/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import java.util.*;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;

public class CalcitePPLMvExpandTest extends CalcitePPLAbstractTest {
  public CalcitePPLMvExpandTest() {
    super(new ReflectiveSchema(new MvExpandSchema()));
    System.out.println("CalcitePPLMvExpandTest constructed!");
  }

  public static class MvExpandSchema {
    public List<User> USERS;

    public MvExpandSchema() {
      System.out.println("MvExpandSchema constructor called!");
      USERS =
          Arrays.asList(
              new User("happy", Arrays.asList(new Skill("python", null), new Skill("java", null))),
              new User("single", Arrays.asList(new Skill("go", null))));
    }
  }

  public static class User {
    public String username;
    public List<Skill> skills;

    public User(String username, List<Skill> skills) {
      System.out.println("User created: " + username + ", skills: " + skills);
      this.username = username;
      this.skills = skills;
    }
  }

  public static class Skill {
    public String name;
    public String level;

    public Skill(String name, String level) {
      System.out.println("Skill created: " + name + ", " + level);
      this.name = name;
      this.level = level;
    }
  }

  @Test
  public void testMvExpand_HappyPath() {
    String ppl = "source=USERS | mvexpand skills";
    RelNode root = getRelNode(ppl);
    System.out.println("testMvExpand_HappyPath ran!");
  }
}
