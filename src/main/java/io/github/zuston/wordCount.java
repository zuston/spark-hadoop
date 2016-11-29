package io.github.zuston;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zuston on 16-11-29.
 */
public class wordCount {

    public static void main(String[] args) {
            String logFile = "./data/data.txt"; // Should be some file on your system
            SparkConf conf = new SparkConf().setAppName("wordCount");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaRDD<String> logData = sc.textFile(logFile).cache();

            long numAs = logData.filter(s->s.contains("a")).count();

            long numBs = logData.filter(s->s.contains("a")).count();

            System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

            sc.stop();
        }

}
