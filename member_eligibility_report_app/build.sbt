
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.13"

lazy val root = (project in file("."))
  .settings(
    name := "member_eligibility_report_app",
    idePackagePrefix := Some("org.eligibility.report")
  )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.4",
  "org.apache.spark" %% "spark-sql" % "3.2.4"
)