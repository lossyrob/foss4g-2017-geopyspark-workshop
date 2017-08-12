organization := "com.azavea"
name := "levee-pointcloud-demo"
version := "0.0.1"
scalaVersion := "2.11.8"
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked",
  "-Yinline-warnings",
  "-language:implicitConversions",
  "-language:reflectiveCalls",
  "-language:higherKinds",
  "-language:postfixOps",
  "-language:existentials",
  "-feature")

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-s3" % "1.2.0-ROB-WORKSHOP", // TODO - Replace with milestone
  "org.apache.spark" %% "spark-core"    % "2.2.0" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.8.0" % "provided"
)

// Allows provided dependencies to be included in the run command
run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated

outputStrategy := Some(StdoutOutput)
fork := true

assemblyJarName in assembly := "foss4g-2017-workshop-ingest.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "reference.conf" | "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}
