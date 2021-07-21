package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class WriteDataFrameToRedis {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("write-redis-df")
                .master("spark://spark-master:7077")
                .config("spark.redis.host", "redis-13200.re-cluster1.ps-redislabs.org")
                .config("spark.redis.port", "13200")
                .getOrCreate();

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                new Person("John", 35),
                new Person("Peter", 40)), Person.class);

        df.schema().add("name", StringType);
        df.schema().add("age", IntegerType);
        df.printSchema();

        df.write()
                .format("org.apache.spark.sql.redis")
                .option("table", "person")
                .option("key.column", "name")
                .mode(SaveMode.Overwrite)
                .save();

    }
}


