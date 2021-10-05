package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class OracleTableLoader {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[*]");
        SparkSession spark = SparkSession
                .builder()
                .appName("redis-df")
                .config(conf)
                .getOrCreate();

        String driver = "oracle.jdbc.OracleDriver";
        String url = "jdbc:oracle:thin:@//localhost:1521/ORCLCDB.localdomain";
        String user = "appuser";
        String pass = "appuser";

        Dataset sourceDf = spark.read().format("jdbc")
                .option("driver", driver)
                .option("url", url)
                .option("query","select * from Record")
                .option("user", user)
                .option("password", pass)
                .option("driver", "oracle.jdbc.OracleDriver")
                .load();

        sourceDf.show();

        sourceDf.write()
                .format("org.apache.spark.sql.redis")
                .option("table", "record")
                .mode("overwrite")
                .save();

    }
}
