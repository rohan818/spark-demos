import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__=="__main__":
    
    # Check if the correct number of arguments is provided
    if len(sys.argv)!=2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)
    
    # Create a SparkSession
    spark = (SparkSession.builder.appName("PythonMnMCount").getOrCreate())
    
    # Get the input file path from command line arguments
    mnm_file = sys.argv[1]
    
    # Read the CSV file into a DataFrame
    mnm_df = (spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(mnm_file))

    # Count the total M&M counts grouped by State and Color, and order by total count in descending order
    count_mnm_df = (mnm_df.select("State", "Color", "Count").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy("Total", ascending=False))

    # Show the top 60 rows of the DataFrame and print the total count of rows
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

    # Count the total M&M counts in California (CA) grouped by Color, and order by total count in descending order
    ca_count_mnm_df = (mnm_df.select("State", "Color", "Count").where(mnm_df.State == "CA").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy("Total", ascending=False))

    # Show the top 10 rows of the DataFrame for California and print the total count of rows
    ca.count_mnm_df.show(n=10, truncate=False)    

    # Stop the SparkSession
    spark.stop()
    
    
