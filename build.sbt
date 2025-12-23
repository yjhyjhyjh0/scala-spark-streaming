ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-streaming",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.0" ,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.0",
      "org.apache.spark" %% "spark-streaming" % "3.5.0"

    )
  )