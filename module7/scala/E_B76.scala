package main.scala.ch7

// Import necessary Spark SQL, Spark Core, and utility libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._
import scala.util.Random

object SortMergeJoinBucketed76 {

  //  function to measure execution time of code blocks
  def benchmark(name: String)(f: => Unit) {
    val startTime = System.nanoTime
    f
    val endTime = System.nanoTime
    println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
  }

  def main (args: Array[String] ) {

    // Initialize SparkSession with specific configurations for performance optimization
    val spark = SparkSession.builder
        .appName("SortMergeJoinBucketed76")
        .config("spark.sql.codegen.wholeStage", true)
        .config("spark.sql.join.preferSortMergeJoin", true)
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.sql.defaultSizeInBytes", 100000)
        .config("spark.sql.shuffle.partitions", 16)
        .getOrCreate ()

    // Import implicits for enabling DataFrame and Dataset APIs
    import spark.implicits._

    // Initialize mutable maps for states and items with sample data
    var states = scala.collection.mutable.Map[Int, String]()
    var items = scala.collection.mutable.Map[Int, String]()
    val rnd = new scala.util.Random(42)

    // Populate the maps with sample data for states and items
    states += (0 -> "AZ", 1 -> "CO", 2-> "CA", 3-> "TX", 4 -> "NY", 5-> "MI")
    items += (0 -> "SKU-0", 1 -> "SKU-1", 2-> "SKU-2", 3-> "SKU-3", 4 -> "SKU-4", 5-> "SKU-5")

    // Create DataFrames for users and orders with simulated data and persist them
    val usersDF = (0 to 100000).map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5)))).toDF("uid", "login", "email", "user_state")
    val ordersDF = (0 to 100000).map(r => (r, r, rnd.nextInt(100000), 10 * r* 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5)))).toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")
    usersDF.persist(DISK_ONLY)
    ordersDF.persist(DISK_ONLY)

    // Create bucketed tables for users and orders to optimize join operations
    spark.sql("DROP TABLE IF EXISTS UsersTbl")
    usersDF.orderBy(asc("uid"))
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .bucketBy(8, "uid") // Bucket by 'uid' column
      .saveAsTable("UsersTbl")

    spark.sql("DROP TABLE IF EXISTS OrdersTbl")
    ordersDF.orderBy(asc("users_id"))
      .write.format("parquet")
      .mode(SaveMode.Overwrite)
      .bucketBy(8, "users_id") // Bucket by 'users_id' column
      .saveAsTable("OrdersTbl")

    // Cache the bucketed tables for faster access during query execution
    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")

    // Retrieve and display data from the bucketed tables
    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")

    // Perform a join operation between the bucketed tables and explain the query plan
    val joinUsersOrdersBucketDF = ordersBucketDF.join(usersBucketDF, $"users_id" === $"uid")
    joinUsersOrdersBucketDF.show(false)
    joinUsersOrdersBucketDF.explain()

    // Keep the application running for debugging or further exploration 
    // Thread.sleep(200000000)
  }
}
