### To execute the Scala code:

#### To build the package
 1. run `sbt clean package`
 2. The generated jar file can be found at: target/scala-2.12/main-scala-ch7_2.12-1.0.jar 

#### To run the Scala code use:

 * ` spark-submit --class main.scala.ch7.SortMergeJoin76 target/scala-2.12/main-scala-ch7_2.12-1.0.jar`
 * ` spark-submit --class main.scala.ch7.SortMergeJoinBucketed76 target/scala-2.12/main-scala-ch7_2.12-1.0.jar`
 
