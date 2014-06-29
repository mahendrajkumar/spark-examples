package com.mahendra.spark.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import java.util.Arrays;
import scala.Tuple2;

/**
 * 
 * @author Mahendra J Kumar
 *
 */
public class Java8WordCount {

    public static void main(String[] args) {
	if (args.length != 2) {
	    System.err.println("Usage: Java8WordCount <input_file> <output_file>");
	    System.exit(1);
	}
	SparkConf conf = new SparkConf().setAppName("WordCount");
	JavaSparkContext sparkContext = new JavaSparkContext(conf);
	JavaRDD<String> lines = sparkContext.textFile(args[0], 1);
	JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")));
	JavaPairRDD<String, Integer> counts = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1)).reduceByKey(
		(x, y) -> x + y);
	counts.saveAsTextFile(args[1]);
	sparkContext.stop();
    }
}
