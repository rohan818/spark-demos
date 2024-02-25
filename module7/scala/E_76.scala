package main.scala.ch7

// Import necessary Spark SQL and Spark Core libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Random

// Define the main object for the Spark application
object SortMergeJoin76 {
    // Define a utility method for benchmarking blocks of code
    def benchmark(name: String)(f: => Unit) {
        val startTime = System.nanoTime
        f
        val endTime = System.nanoTime
        println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
    }

    // Main method - entry point for the Spark application
    def main(args: Array[String]) { 

        // Initialize SparkSession with necessary configurations for performance optimization
        val spark = SparkSession.builder
            .appName("SortMergeJoin76") 
            .config("spark.sql.codegen.wholeStage", true)
            .config("spark.sql.join.preferSortMergeJoin", true)
            .config("spark.sql.autoBroadcastJoinThreshold", -1)
            .config("spark.sql.defaultSizeInBytes", 100000)
            .config("spark.sql.shuffle.partitions", 16)
            .getOrCreate()

        // Import implicits for enabling DataFrame and Dataset APIs
        import spark.implicits._ 

        // Initialize mutable maps for states and items with sample data
        var states = scala.collection.mutable.Map[Int, String]()
        var items = scala.collection.mutable.Map[Int, String]()
        val rnd = new scala.util.Random(42)

        // Populate maps with sample state and item data
        states += (0 -> "AZ", 1 -> "CO", 2 -> "CA", 3 -> "TX", 4 -> "NY", 5 -> "MI")
        items += (0 -> "SKU-0", 1 -> "SKU-1", 2 -> "SKU-2", 3 -> "SKU-3", 4 -> "SKU-4", 5 -> "SKU-5")

        // Create DataFrames for users and orders with simulated data
        val usersDF = (0 to 100000).map(id => (id, s"user_$id", s"user_$id@databricks.com", states(rnd.nextInt(5)))).toDF("uid", "login", "email", "user_state")
        val ordersDF = (0 to 100000).map(r => (r, r, rnd.nextInt(100000), 10 * r * 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5)))).toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

        // Show sample data from users and orders DataFrames
        usersDF.show(10)
        ordersDF.show(10)

        // Perform a join operation between users and orders DataFrames
        val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")
        
        // Show results of the join and cache the resulting DataFrame for performance
        usersOrdersDF.show(10, false)
        usersOrdersDF.cache()
        
        // Explain the physical plan for the join operation for optimization insights
        usersOrdersDF.explain()
    }
}
