.. highlight:: sh

===============
Developer Guide
===============

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 2


Prerequisites
=============

JDK
---

OpenSearch SQL plugin requires Java 21 for development and runtime. This means you must have JDK 21 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK installation::

   $ echo $JAVA_HOME
   /Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home

   $ java -version
   openjdk version "21.0.8" 2024-07-16 LTS
   OpenJDK Runtime Environment (build 21.0.8+13-LTS)
   OpenJDK 64-Bit Server VM (build 21.0.8+13-LTS, mixed mode, sharing)

Here are the official instructions on how to set ``JAVA_HOME`` for different platforms: https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/. 

OpenSearch & OpenSearch Dashboards
----------------------------------

For convenience, we recommend installing `OpenSearch <https://www.opensearch.org/downloads.html>`_ and `OpenSearch Dashboards <https://www.opensearch.org/downloads.html>`_ on your local machine. You can download the open source ZIP for each and extract them to a folder.

If you just want to have a quick look, you can also get an OpenSearch running with plugin installed by ``./gradlew :opensearch-sql-plugin:run``.

OpenSearch Dashboards is optional, but makes it easier to test your queries. Alternately, you can use curl from the terminal to run queries against the plugin.

Getting Source Code
===================

Now you can check out the code from your forked GitHub repository and create a new branch for your bug fix or enhancement work::

   $ git clone git@github.com:<your_account>/sql.git
   $ git checkout -b <branch_name>

If there is update in main or you want to keep the forked repository long living, you can sync it by following the instructions: https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork. Basically you just need to pull latest changes from upstream main once you add it for the first time::

   #Merge to your local main
   $ git fetch upstream
   $ git checkout main
   $ git merge upstream/main

   #Merge to your branch if any
   $ git checkout <branch_name>
   $ git merge main

After getting the source code as well as OpenSearch and OpenSearch Dashboards, your workspace layout may look like this::

   $ mkdir opensearch
   $ cd opensearch
   $ ls -la                                                                     
   total 32
   drwxr-xr-x  7 user group^users 4096 Nov 21 12:59 .
   drwxr-xr-x 19 user group^users 4096 Nov 21 09:44 ..
   drwxr-xr-x 10 user group^users 4096 Nov  8 12:16 opensearch
   drwxr-xr-x 14 user group^users 4096 Nov  8 12:14 opensearch-dashboards
   drwxr-xr-x 16 user group^users 4096 Nov 15 10:59 sql


Configuring IDEs
================

You can develop the plugin in your favorite IDEs such as Eclipse and IntelliJ IDEs. Before start making any code change, you may want to configure your IDEs. In this section, we show how to get IntelliJ up and running.

Java Language Level
-------------------

The plugin requires Java 21 for both development and runtime. Make sure your IDE is configured to use Java 21 as the project SDK and language level. In case your IDE doesn't set it right, you may want to double check your project setting after import.

Remote Debugging
----------------

Firstly you need to add the following configuration to the JVM used by your IDE. For Intellij IDEA, it should be added to ``<OpenSearch installation>/config/jvm.options`` file. After configuring this, an agent in JVM will listen on the port when your OpenSearch bootstraps and wait for IDE debugger to connect. So you should be able to debug by setting up a "Remote Run/Debug Configuration"::

   -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

This is automatically applied if you pass the ``debugJVM`` flag when
running.

::

   ./gradlew opensearch-sql:run -DdebugJVM

To connect to the cluster with the debugger in an IDE, you'll need to
connect to that port. For IntelliJ, see `attaching to a remote process <https://www.jetbrains.com/help/idea/attach-to-process.html#attach-to-remote>`_.

License Header
--------------

Because our code is licensed under Apache 2, you need to add the following license header to all new source code files. To automate this whenever creating new file, you can follow instructions for your IDE.

.. code:: java

  /*
   * Copyright OpenSearch Contributors
   * SPDX-License-Identifier: Apache-2.0
   */

For example, `here are the instructions for adding copyright profiles in IntelliJ IDEA <https://www.jetbrains.com/help/idea/copyright.html>`__.

Note that missing license header will be detected by Gradle license plugin and fails the build.


Making Code Changes
===================

Project Structure
-----------------

The plugin codebase is in standard layout of Gradle project::

   .
   ├── CODE_OF_CONDUCT.md
   ├── CONTRIBUTING.md
   ├── LICENSE.TXT
   ├── NOTICE
   ├── README.md
   ├── THIRD-PARTY
   ├── build.gradle
   ├── config
   ├── docs
   │   ├── attributions.md
   │   ├── category.json
   │   ├── dev
   │   └── user
   ├── gradle.properties
   ├── gradlew
   ├── gradlew.bat
   ├── settings.gradle
   ├── common
   ├── core
   ├── doctest
   ├── opensearch
   ├── prometheus
   ├── integ-test
   ├── legacy
   ├── plugin
   ├── protocol
   ├── ppl
   ├── sql
   ├── sql-cli
   ├── sql-jdbc
   ├── sql-odbc
   └── workbench

Here are sub-folders (Gradle modules) for plugin source code:

- ``plugin``: OpenSearch plugin related code.
- ``sql``: SQL language processor.
- ``ppl``: PPL language processor.
- ``core``: core query engine.
- ``opensearch``: OpenSearch storage engine.
- ``prometheus``: Prometheus storage engine.
- ``protocol``: request/response protocol formatter.
- ``common``: common util code.
- ``integ-test``: integration and comparison test.

Here are other files and sub-folders that you are likely to touch:

- ``build.gradle``: Gradle build script.
- ``docs``: documentation for developers and reference manual for users.
- ``doc-test``: code that run .rst docs in ``docs`` folder by Python doctest library.

Note that other related project code has already merged into this single repository together:

- ``sql-cli``: CLI tool for running query from command line.
- ``sql-jdbc``: JDBC driver.
- ``sql-odbc``: ODBC driver.
- ``workbench``: query workbench UI.


Code Convention
---------------

Java files in the OpenSearch codebase are formatted with the Eclipse JDT formatter, using the `Spotless Gradle <https://github.com/diffplug/spotless/tree/master/plugin-gradle>`_ plugin. This plugin is configured in the project  `./gradle.properties`.

The formatting check can be run explicitly with::

./gradlew spotlessCheck

The code can be formatted with::

./gradlew spotlessApply

These tasks can also be run for specific modules, e.g.::

./gradlew server:spotlessCheck

For more information on the spotless for the OpenSearch project please see `https://github.com/opensearch-project/OpenSearch/blob/main/DEVELOPER_GUIDE.md#java-language-formatting-guidelines <https://github.com/opensearch-project/OpenSearch/blob/main/DEVELOPER_GUIDE.md#java-language-formatting-guidelines>`_.

Java files are formatted using `Spotless <https://github.com/diffplug/spotless>`_ conforming to `Google Java Format <https://github.com/google/google-java-format>`_.
   * - New line at end of file
   * - No unused import statements
   * - Fix import order to be alphabetical with static imports first (one block for static and one for non-static imports)
   * - Max line length is 100 characters (does not apply to import statements)
   * - Line spacing is 2 spaces
   * - Javadocs should be properly formatted in accordance to `Javadoc guidelines <https://www.oracle.com/ca-en/technical-resources/articles/java/javadoc-tool.html>`_
   * - Javadoc format can be maintained by wrapping javadoc with `<pre></pre>` HTML tags
   * - Strings can be formatted on multiple lines with a `+` with the correct indentation for the string.

Development Guidelines
----------------------

For detailed development documentation, please refer to the `development documentation <docs/dev/index.md>`_. For specific guidance on implementing PPL components, see the following resources:

- `PPL Commands <docs/dev/ppl-commands.md>`_: Guidelines for adding new commands to PPL
- `PPL Functions <docs/dev/ppl-functions.md>`_: Instructions for implementing and integrating custom functions

Building and Running Tests
==========================

Gradle Build
------------

Most of the time you just need to run ``./gradlew build`` which will make sure you pass all checks and testing. While you're developing, you may want to run specific Gradle task only. In this case, you can run ./gradlew with task name which only triggers the task along with those it depends on. Here is a list for common tasks:

For faster local iterations, skip integration tests. ``./gradlew build -x integTest``.

.. list-table::
   :widths: 30 50
   :header-rows: 1

   * - Gradle Task
     - Description
   * - ./gradlew assemble
     - Generate jar and zip files in build/distributions folder.
   * - ./gradlew generateGrammarSource
     - (Re-)Generate ANTLR parser from grammar file.
   * - ./gradlew compileJava
     - Compile all Java source files.
   * - ./gradlew test
     - Run all unit tests.
   * - ./gradlew :integ-test:integTest
     - Run all integration test (this takes time).
   * - ./gradlew :integ-test:yamlRestTest
     - Run rest integration test.
   * - ./gradlew :doctest:doctest
     - Run doctests in docs folder. You can use ``-Pdocs=file1,file2`` to run specific file(s). See more info in `Documentation <#documentation>`_ section.
   * - ./gradlew build
     - Build plugin by run all tasks above (this takes time).
   * - ./gradlew pitest
     - Run PiTest mutation testing (see more info in `#1204 <https://github.com/opensearch-project/sql/pull/1204>`_)
   * - ./gradlew spotlessCheck
     - Runs Spotless to check for code style.
   * - ./gradlew spotlessApply
     - Automatically apply spotless code style changes.

For integration test, you can use ``-Dtests.class`` "UT full path" to run a task individually. For example ``./gradlew :integ-test:integTest -Dtests.class="*QueryIT"``.

If Prometheus isn't available in your environment, you can skip downloading and starting it by adding ``-DignorePrometheus`` (or setting it to any value other than ``false``) to the command. For example ``./gradlew :integ-test:integTest -DignorePrometheus`` bypasses Prometheus setup and excludes Prometheus-specific integration tests, and ``./gradlew :doctest:doctest -DignorePrometheus`` skips the Prometheus-dependent doctest cases.

To run the task above for specific module, you can do ``./gradlew :<module_name>:task``. For example, only build core module by ``./gradlew :core:build``.

Troubleshooting
---------------

Sometimes your Gradle build fails or timeout due to OpenSearch integration test process hung there. You can check this by the following commands::

   #Check if multiple Gradle daemons started by different JDK.
   #Kill unnecessary ones and restart if necessary.
   $ ps aux | grep -i gradle
   $ ./gradlew stop
   $ ./gradlew start

   #Check if OpenSearch integTest process hung there. Kill it if so.
   $ ps aux | grep -i opensearch

   #Clean and rebuild
   $ ./gradlew clean
   $ ./gradlew build

Tips for Testing
----------------

For test cases, you can use the cases in the following checklist in case you miss any important one and break some queries:

- *Functions*

  - SQL functions
  - Special OpenSearch functions
  
- *Basic Query*

  - SELECT-FROM-WHERE
  - GROUP BY & HAVING
  - ORDER BY
  
- *Alias*

  - Table alias
  - Field alias
  
- *Complex Query*

  - Subquery: IN/EXISTS
  - JOIN: INNER/LEFT OUTER.
  - Nested field query
  - Multi-query: UNION/MINUS
  
- *Other Statements*

  - SHOW
  - DESCRIBE
  
- *Explain*

  - DSL for simple query
  - Execution plan for complex query like JOIN
  
- *Response format*

  - Default
  - JDBC: You could set up DbVisualizer or other GUI.
  - CSV
  - Raw

For unit test:

* Put your test class in the same package in src/test/java so you can access and test package-level method.
* Make sure you are testing against the right abstraction with dependencies mocked. For example a bad practice is to create many classes by OpenSearchActionFactory class and write test cases on very high level. This makes it more like an integration test.

For integration test:

* OpenSearch test framework is in use so an in-memory cluster will spin up for each test class.
* You can only access the plugin and verify the correctness of your functionality via REST client externally.
* Our homemade comparison test framework is used heavily to compare with other databases without need of assertion written manually. More details can be found in `Testing <./dev/Testing.md>`_.

Here is a sample for integration test for your reference:

.. code:: java

   public class XXXIT extends SQLIntegTestCase { // Extends our base test class
   
       @Override
       protected void init() throws Exception {
           loadIndex(Index.ACCOUNT); // Load predefined test index mapping and data
       }
   
       @Override
       public void testXXX() { // Test query against the index and make assertion
           JSONObject response = executeQuery("SELECT ...");
           Assert.assertEquals(6, getTotalHits(response));
       }
   }

Finally thanks to JaCoCo library, you can check out the test coverage in ``<module_name>/build/reports/jacoco`` for your changes easily.

Deploying Locally
-----------------

Sometime you want to deploy your changes to local OpenSearch cluster, basically there are couple of steps you need to follow:

1. Re-assemble to generate plugin jar file with your changes.
2. Replace the jar file with the new one in your workspace.
3. Restart OpenSearch cluster to take it effect.


To automate this common task, you can prepare an all-in-one command for reuse. Below is a sample command for macOS::

 ./gradlew assemble && {echo y | cp -f build/distributions/opensearch-sql-1*0.jar <OpenSearch_home>/plugins/opensearch-sql} && {kill $(ps aux | awk '/[O]pensearch/ {print $2}'); sleep 3; nohup <OpenSearch_home>/bin/opensearch > ~/Temp/opensearch.log 2>&1 &}

Note that for the first time you need to create ``opensearch-sql`` folder and unzip ``build/distribution/opensearch-sql-xxxx.zip`` to it.


Documentation
=============

Dev Docs
--------

For new feature or big enhancement, it is worth document your design idea for others to understand your code better. There is already a docs/dev folder for all this kind of development documents.

Reference Manual
----------------

Doc Generator
>>>>>>>>>>>>>

Currently the reference manual documents are generated from a set of special integration tests. The integration tests use custom DSL to build ReStructure Text markup with real query and result set captured and documented.

1. Add a new template to ``src/test/resources/doctest/templates``.
2. Add a new test class as below with ``@DocTestConfig`` annotation specifying template and test data used.
3. Run ``./gradlew build`` to generate the actual documents into ``docs/user`` folder.

Sample test class:

.. code:: java

   @DocTestConfig(template = "interfaces/protocol.rst", testData = {"accounts.json"})
   public class ProtocolIT extends DocTest {
   
       @Section(1)
       public void test() {
           section(
               title("A New Section"),
               description(
                   "Describe what is the use of new functionality."
               ),
               example(
                   description("Describe what is the use case of this example to show"),
                   post("SELECT ...")
               )
           );
       }
   }

Doctest
>>>>>>>

Python doctest library makes our document executable which keeps it up-to-date to source code. The doc generator aforementioned served as scaffolding and generated many docs in short time. Now the examples inside is changed to doctest gradually. For more details please read `testing-doctest <./docs/dev/testing-doctest.md>`_.

.. code-block:: bash
   # Test all docs
   ./gradlew :doctest:doctest

   # Test single file using main doctest task
   ./gradlew :doctest:doctest -Pdocs=search
   
   # Test multiple files at once
   ./gradlew :doctest:doctest -Pdocs=search,fields,basics
   
   # With verbose output
   ./gradlew :doctest:doctest -Pdocs=stats -Pverbose=true

Backports
>>>>>>>>>

The Github workflow in `backport.yml <.github/workflows/backport.yml>`_ creates backport PRs automatically when the original PR
with an appropriate label `backport <backport-branch-name>` is merged to main with the backport workflow run successfully on the
PR. For example, if a PR on main needs to be backported to `1.x` branch, add a label `backport 1.x` to the PR and make sure the
backport workflow runs on the PR along with other checks. Once this PR is merged to main, the workflow will create a backport PR
to the `1.x` branch.
