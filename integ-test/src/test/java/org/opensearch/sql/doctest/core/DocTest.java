/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.doctest.core;

import static com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import static java.nio.file.StandardOpenOption.APPEND;
import static org.opensearch.test.OpenSearchIntegTestCase.Scope.SUITE;

import com.carrotsearch.randomizedtesting.AnnotatedMethodProvider;
import com.carrotsearch.randomizedtesting.TestMethodAndParams;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import org.opensearch.client.RestClient;
import org.opensearch.common.Strings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.sql.doctest.core.annotation.DocTestConfig;
import org.opensearch.sql.doctest.core.annotation.Section;
import org.opensearch.sql.doctest.core.builder.DocBuilder;
import org.opensearch.sql.doctest.core.markup.Document;
import org.opensearch.sql.doctest.core.markup.RstDocument;
import org.opensearch.sql.legacy.CustomExternalTestCluster;
import org.opensearch.sql.legacy.TestUtils;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.TestCluster;

/**
 * Documentation test base class
 */
@TestMethodProviders({DocTest.SectionMethod.class})
@TestCaseOrdering(DocTest.SectionOrder.class)
@OpenSearchIntegTestCase.SuiteScopeTestCase
@ClusterScope(scope = SUITE, numDataNodes = 1, supportsDedicatedMasters = false, transportClientRatio = 1)
@ThreadLeakScope(Scope.NONE)
public abstract class DocTest extends OpenSearchIntegTestCase implements DocBuilder {

  @Override
  protected void setupSuiteScopeCluster() {
    DocTestConfig config = getClass().getAnnotation(DocTestConfig.class);
    loadTestData(config);
    copyTemplateToDocument(config);
  }

  @Override
  public RestClient restClient() {
    return getRestClient();
  }

  @Override
  public Document openDocument() {
    DocTestConfig config = getClass().getAnnotation(DocTestConfig.class);
    Path docPath = absolutePath(config.template());
    try {
      PrintWriter docWriter = new PrintWriter(Files.newBufferedWriter(docPath, APPEND));
      return new RstDocument(docWriter);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to open document file " + docPath, e);
    }
  }

  private void loadTestData(DocTestConfig config) {
    String[] testFilePaths = config.testData();
    TestData testData = new TestData(testFilePaths);
    testData.loadToES(this);
  }

  private void copyTemplateToDocument(DocTestConfig config) {
    Path docPath = absolutePath(config.template());
    Template template = new Template(config.template());
    template.copyToDocument(docPath);
  }

  /**
   * Method annotated by {@link Section} will be treated as test method.
   */
  public static class SectionMethod extends AnnotatedMethodProvider {
    public SectionMethod() {
      super(Section.class);
    }
  }

  /**
   * Test methods will execute in order defined by value in {@link Section} annotation.
   */
  public static class SectionOrder implements Comparator<TestMethodAndParams> {
    @Override
    public int compare(TestMethodAndParams method1, TestMethodAndParams method2) {
      return Integer.compare(order(method1), order(method2));
    }

    private int order(TestMethodAndParams method) {
      Section section = method.getTestMethod().getAnnotation(Section.class);
      return section.value();
    }
  }

  private Path absolutePath(String templateRelativePath) {
    return Paths.get(TestUtils.getResourceFilePath(DOCUMENT_FOLDER_ROOT + templateRelativePath));
  }

  @Override
  protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {

    String clusterAddresses = System.getProperty(TESTS_CLUSTER);

    if (Strings.hasLength(clusterAddresses)) {
      String[] stringAddresses = clusterAddresses.split(",");
      TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
      int i = 0;
      for (String stringAddress : stringAddresses) {
        URL url = new URL("http://" + stringAddress);
        InetAddress inetAddress = InetAddress.getByName(url.getHost());
        transportAddresses[i++] =
            new TransportAddress(new InetSocketAddress(inetAddress, url.getPort()));
      }
      return new CustomExternalTestCluster(createTempDir(), externalClusterClientSettings(),
          transportClientPlugins(), transportAddresses);
    }
    return super.buildTestCluster(scope, seed);
  }

}
