/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
import Dependencies._

lazy val scala212 = "2.12.14"
lazy val sparkVersion = "3.3.1"
lazy val opensearchVersion = "2.6.0"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := scala212

ThisBuild / scalafmtConfig := baseDirectory.value / "dev/.scalafmt.conf"

/**
 * ScalaStyle configurations
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

// Run as part of compile task.
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

// Run as part of test task.
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

lazy val build = taskKey[Unit]("assemblyWithCoverage")

build := Def
  .sequential(flintSparkIntegration / assembly, flintSparkIntegration / coverageReport)
  .value

lazy val commonSettings = Seq(
  // Coverage
  // todo. for demo now, increase to 100.
  coverageMinimumStmtTotal := 70,
  // todo. for demo now, increase to 100.
  coverageMinimumBranchTotal := 70,
  coverageFailOnMinimum := true,
  coverageEnabled := true,

  // Scalastyle
  scalastyleConfig := (ThisBuild / scalastyleConfig).value,
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  Test / test := ((Test / test) dependsOn testScalastyle).value)

lazy val root = (project in file("."))
  .aggregate(flintCore, flintSparkIntegration)
  .disablePlugins(AssemblyPlugin)
  .settings(name := "flint")

lazy val flintCore = (project in file("flint-core"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "flint-core",
    scalaVersion := scala212,
    // todo, does it required?
    javacOptions ++= Seq("-source", "11", "-target", "11"),
    libraryDependencies ++= Seq(
      "org.opensearch.client" % "opensearch-rest-client" % opensearchVersion,
      "org.opensearch.client" % "opensearch-rest-high-level-client" % opensearchVersion))

lazy val flintSparkIntegration = (project in file("flint-spark-integration"))
  .dependsOn(flintCore)
  .enablePlugins(AssemblyPlugin)
  .settings(
    commonSettings,
    name := "flint-spark-integration",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.opensearch.client" % "opensearch-rest-client" % opensearchVersion,
      "org.opensearch.client" % "opensearch-rest-high-level-client" % opensearchVersion,
      "org.json4s" %% "json4s-jackson" % "3.7.0-M11" % "provided",
      "org.testcontainers" % "testcontainers" % "1.18.0" % "test",
      "org.scalactic" %% "scalactic" % "3.2.15",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
      "org.scalatest" %% "scalatest-flatspec" % "3.2.15" % "test",
      "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % "test",
      "com.github.sbt" % "junit-interface" % "0.13.3" % "test"),
    libraryDependencies ++= deps(sparkVersion),
    assemblyPackageScala / assembleArtifact := false,
    assembly / assemblyOption ~= {
      _.withIncludeScala(false)
    },
    assembly / assemblyMergeStrategy := {
      case PathList(ps @ _*) if ps.last endsWith ("module-info.class") =>
        MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", "versions", xs @ _, "module-info.class") =>
        MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    assembly / test := (Test / test).value)

// Test assembly package with integration test.
lazy val integtest = (project in file("integ-test"))
  .dependsOn(flintSparkIntegration % "test->test")
  .settings(
    commonSettings,
    name := "integ-test",
    scalaVersion := scala212,
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.2.15",
      "org.scalatest" %% "scalatest" % "3.2.15" % "test"),
    libraryDependencies ++= deps(sparkVersion),
    Test / fullClasspath += (flintSparkIntegration / assembly).value)
