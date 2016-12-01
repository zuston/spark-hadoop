package io.github.zuston;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by zuston on 16-11-30.
 */
public class average {
    public static void main(String[] args) {
        String logFile = "./data/data.txt"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("average");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile);


        JavaPairRDD<String,Integer> pairRDD = logData.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        JavaPairRDD<String,Integer> filterRdd = pairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._1().length()>100;
            }
        });

        for (Tuple2<String,Integer> fil:filterRdd.take(10)){
            System.out.println(fil._1);
        }
    }
}
