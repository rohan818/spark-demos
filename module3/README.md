
### To execute the Python code run:

 * `spark-submit E3_6.py`

### To execute the Scala code:

#### To build the package
 1. sbt clean package
 2. The generated jar file can be found at: target/scala-2.12/main-scala-chapter3_2.12-1.0.jar jars/

#### To run the Example
To run the Scala code use:

 * `spark-submit --class main.scala.ch3.E3_7 target/scala-2.12/main-scala-ch3_2.12-1.0.jar blogs.json`
 