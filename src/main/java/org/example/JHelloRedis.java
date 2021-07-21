package org.example;

import java.util.Arrays;
import org.apache.spark.SparkConf;

import com.redislabs.provider.redis.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

public class JHelloRedis {

        public static void main(String[] args) {
            SparkConf sparkConf = new SparkConf()
                    .setAppName("MyApp")
                    .setMaster("local[*]")
                    .set("spark.redis.host", "localhost")
                    .set("spark.redis.port", "6379");

            RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
            ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);

            JavaSparkContext jsc = new JavaSparkContext(sparkConf);
            RedisContext redisContext = new RedisContext(jsc.sc());

            JavaRDD<Tuple2<String, String>> rdd = jsc.parallelize(Arrays.asList(Tuple2.apply("myKey", "Hello")));
            int ttl = 0;

            redisContext.toRedisKV(rdd.rdd(), ttl, redisConfig, readWriteConfig);
            RDD<Tuple2<String, String>> hashes = redisContext.
                    fromRedisHash("custom*",3,redisConfig,
                    readWriteConfig);
            long totalHashes = hashes.count();
            System.out.println("total hashes:" + totalHashes);

        }
    }

