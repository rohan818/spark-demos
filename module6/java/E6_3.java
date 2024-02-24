package main.java.ch6;

// Importing necessary Spark SQL classes and other utilities
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import org.apache.commons.lang3.RandomStringUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import static org.apache.spark.sql.functions.*;

public class E6_3 {
    public static int main(String[] args){
        // Initialize SparkSession with an application name
        SparkSession spark = SparkSession.builder().appName("E6_3").getOrCreate();
        
        // Encoder for the Usage class to enable Dataset operations
        Encoder<Usage> usageEncoder = Encoders.bean(Usage.class);
        
        // Random generator for generating mock data
        Random rand = new Random();
        rand.setSeed(42); // Setting seed for reproducibility
        List<Usage> data = new ArrayList<Usage>();

        // Generating 1000 Usage objects with random data
        for (int i=0; i<1000; i++){
            Usage u = new Usage(i, "user-" + RandomStringUtils.randomAlphanumeric(5), rand.nextInt(1000));
            data.add(u);
        }

        // Creating a Dataset from the list of Usage objects
        Dataset<Usage> dsUsage = spark.createDataset(data, usageEncoder);
        dsUsage.show(10); // Displaying the first 10 rows of the Dataset

        // FilterFunction to filter out Usage objects with usage > 900
        FilterFunction<Usage> f = new FilterFunction<Usage>(){
            public boolean call(Usage u){
                return (u.usage > 900);
            }
        };

        // Applying the filter function and ordering the filtered results by descending usage
        dsUsage.filter(f).orderBy(col("usage").desc()).show(5);

        // Mapping Usage objects to their usage value with a simple calculation based on a condition
        dsUsage.map((MapFunction<Usage, Double>) u -> {
            if (u.usage > 750)
                return u.usage * 0.15;
            else
                return u.usage * 0.50;
        }, Encoders.DOUBLE()).show(5);

        // Encoder for the UsageCost class to enable Dataset operations
        Encoder<UsageCost> usageCostsEncoder = Encoders.bean(UsageCost.class);

        // Transforming Usage objects into UsageCost objects with a calculated cost based on usage
        dsUsage.map( (MapFunction<Usage, UsageCost>) u -> {
            double v = 0.0;
            if (u.usage > 750) v = u.usage * 0.15; else v = u.usage * 0.50;
            return new UsageCost(u.uid, u.uname, u.usage, v); }, usageCostsEncoder).show(5);
        
        return 0;
    }
}
