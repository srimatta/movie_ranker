name := "movie_ranker"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.spark" %% "spark-sql" % "3.5.3",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  "org.scalatest" %% "scalatest-funsuite" % "3.2.10" % Test,
  "com.typesafe" % "config" % "1.4.2"
)

// For sbt-assembly
assemblyJarName in assembly := "movie_ranker-assembly-1.0.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
