name := "MORE_DEVELOPMENT"

version := "0.2"

scalaVersion := "2.12.20"

// Spark + Excel dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.6",
  "org.apache.spark" %% "spark-sql"  % "3.5.6",

  // Excel dependency (Spark Excel)
  //"com.crealytics" %% "spark-excel" % "2.4.8_0.20.4"
  // Excel connector – version line is IMPORTANT
  "com.crealytics" %% "spark-excel" % "3.5.1_0.20.4" excludeAll(
    ExclusionRule(organization = "org.apache.spark")
    ),
  // YAML parser
  "org.yaml" % "snakeyaml" % "2.2"
)

// Set the main class
Compile / mainClass := Some("com.newdevelopment.spark.RawToCurratedExcel")

// Assembly settings
import sbtassembly.AssemblyPlugin.autoImport.*
import scala.collection.Seq

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Optional: set jar name for assembly
assembly / artifactName := { (sv, module, artifact) =>
  s"${module.name}-assembly-${module.revision}.jar"
}
