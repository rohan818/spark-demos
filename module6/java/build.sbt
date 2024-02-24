name := "main/java/ch6"

version := "1.0"

scalaVersion := "2.12.18"

ThisBuild / javacOptions ++= Seq("--release", "11")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.apache.spark" %% "spark-sql"  % "3.5.0"
)



// spark-submit --class main.java.ch6.E6_3 target/scala-2.12/main-java-ch6_2.12-1.0.jar