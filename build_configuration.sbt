name := "SparkScalaCourse"

version := "0.2"

scalaVersion := "2.12.20"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.6",
  "org.apache.spark" %% "spark-sql" % "3.5.6",
  "org.apache.spark" %% "spark-mllib" % "3.5.6",
  "org.apache.spark" %% "spark-streaming" % "3.5.6",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "com.crealytics" %% "spark-excel" % "3.5.1_0.20.4",
  "org.yaml" % "snakeyaml" % "2.2"
)



==================================================================

name := "SparkScalaCourse"

version := "0.2"

scalaVersion := "2.12.20"

// Spark dependencies (provided scope when submitting to cluster; change if you need local test jar)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.6" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.5.6" % "provided"
)

// Set the main class for both assembly and standard packaging
Compile / mainClass := Some("com.sundogsoftware.spark.development.RawToCuratedJob.RawToCurated_New")

// assembly settings (optional fine-tuning)
import sbtassembly.AssemblyPlugin.autoImport._
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Optional: set jar name for assembly
assembly / artifactName := { (sv, module, artifact) =>
  s"${module.name}-assembly-${module.revision}.jar"
}
==============================================================
