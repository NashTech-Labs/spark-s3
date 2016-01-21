name := "spark-s3"

organization := "com.knoldus"

scalaVersion := "2.10.4"

version := "1.0.0"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.10.47",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.specs2" %% "specs2" % "3.3.1" % "test",
  "org.specs2" %% "specs2-mock" % "3.7" % "test",
  "org.mockito" % "mockito-core" % "2.0.31-beta"
)
