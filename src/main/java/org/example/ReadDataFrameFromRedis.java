package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadDataFrameFromRedis {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("redis-df")
                .master("local[*]")
                .config("spark.redis.host", "localhost")
                .config("spark.redis.port", "6379")
                .getOrCreate();


        Dataset<Row> df = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("partitions.number",3)
                .option("keys.pattern", "customer:*")
                .option("infer.schema", true)
                .load();

        df.printSchema();
        df.show();
        System.out.println(df.count());
        df.foreachPartition(t -> {
            int count=0;
            System.out.println("partition");
            while (t.hasNext()){
                Row row = (Row) t.next();
                count+=1;
            }
            System.out.println("partition keys:" + count);
        });
    }
}
