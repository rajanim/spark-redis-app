package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class JRedisSparkDataFrame {
    public static void main(String[] args) {
       SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("redis")
                .config("spark.redis.host", "localhost")
                .config("spark.redis.port", "6379")
                .getOrCreate();

        Dataset<Row> df = sparkSession.createDataFrame(Arrays.asList(
                new Person("John", 35),
                new Person("Peter", 40)), Person.class);

        df.schema().add("name", StringType);
        df.schema().add("age", IntegerType);
        df.schema().add("id", StringType);
        df.printSchema();
        df.write()
                .format("org.apache.spark.sql.redis")
                .option("table", "person")
                .option("key.column", "id")
                .mode(SaveMode.Overwrite)
                .save();

        Dataset<Row> loadedDf = sparkSession.read()
                .format("org.apache.spark.sql.redis")
                .option("table", "person")
                .load();
        loadedDf.printSchema();
        loadedDf.show();

        /*Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.redis")
                .option("key.column", "id")
                .option("table", "person")
                .load();
        System.out.println("printing results");
        System.out.println("count: " + df.count());
        df.printSchema();
        df.foreachPartition(t -> {
            while (t.hasNext()){
                Row row = (Row) t.next();
                System.out.println(row.toString());
            }
        });
*/
    }
}



//.config("spark.driver.allowMultipleContexts", "true").config("spark.kryoserializer.buffer.max", "1g")
//   .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
// .config("spark.redis.max.pipeline.size", "10000")
//.config("spark.redis.scan.count", "100000")
  /*  StructType structType = new StructType();
        structType.add("id", "String");
        structType.add("name", "String");*/
/* .schema(
                      structType
                )*/
//.option("keys.pattern", "person:*")