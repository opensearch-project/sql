/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.grok;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import com.google.common.io.Resources;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;
import org.opensearch.sql.common.grok.exception.GrokException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private GrokCompiler compiler;

  @Before
  public void setup() throws Exception {
    compiler = GrokCompiler.newInstance();
    compiler.register(Resources.getResource(ResourceManager.PATTERNS).openStream());
  }

  @Test
  public void test001_compileFailOnInvalidExpression() throws GrokException {
    List<String> badRegxp = new ArrayList<>();
    badRegxp.add("[");
    badRegxp.add("[foo");
    badRegxp.add("?");
    badRegxp.add("foo????");
    badRegxp.add("(?-");

    boolean thrown = false;

    // This should always throw
    for (String regx : badRegxp) {
      try {
        compiler.compile(regx);
      } catch (PatternSyntaxException e) {
        thrown = true;
      }
      assertTrue(thrown);
      thrown = false;
    }
  }

  @Test
  public void test002_compileSuccessValidExpression() throws GrokException {
    List<String> regxp = new ArrayList<>();
    regxp.add("[hello]");
    regxp.add("(test)");
    regxp.add("(?:hello)");
    regxp.add("(?=testing)");

    for (String regx : regxp) {
      compiler.compile(regx);
    }
  }

  @Test
  public void test003_samePattern() throws GrokException {
    String pattern = "Hello World";
    Grok grok = compiler.compile(pattern);
    assertEquals(pattern, grok.getOriginalGrokPattern());
  }

  @Test
  public void test004_sameExpantedPatern() throws GrokException {
    compiler.register("test", "hello world");
    Grok grok = compiler.compile("%{test}");
    assertEquals("(?<name0>hello world)", grok.getNamedRegex());
  }

  @Test
  public void test005_testLoadPatternFromFile() throws IOException, GrokException {
    File temp = tempFolder.newFile("grok-tmp-pattern");
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(temp))) {
      bw.write("TEST \\d+");
    }

    GrokCompiler compiler = GrokCompiler.newInstance();
    compiler.register(new FileInputStream(temp));
    Grok grok = compiler.compile("%{TEST}");
    assertEquals("(?<name0>\\d+)", grok.getNamedRegex());
  }

  @Test
  public void test006_testLoadPatternFromFileIso_8859_1() throws IOException, GrokException {
    File temp = tempFolder.newFile("grok-tmp-pattern");
    try (FileOutputStream fis = new FileOutputStream(temp);
        BufferedWriter bw =
            new BufferedWriter(new OutputStreamWriter(fis, StandardCharsets.ISO_8859_1))) {
      bw.write("TEST \\u2022");
    }

    GrokCompiler compiler = GrokCompiler.newInstance();
    compiler.register(new FileInputStream(temp), StandardCharsets.ISO_8859_1);
    Grok grok = compiler.compile("%{TEST}");
    assertEquals("(?<name0>\\u2022)", grok.getNamedRegex());
  }

  @Test
  public void test007_testLoadPatternFromReader() throws IOException, GrokException {
    Reader reader = new StringReader("TEST \\u20AC");
    GrokCompiler compiler = GrokCompiler.newInstance();
    compiler.register(reader);
    Grok grok = compiler.compile("%{TEST}");
    assertEquals("(?<name0>\\u20AC)", grok.getNamedRegex());
  }
}
