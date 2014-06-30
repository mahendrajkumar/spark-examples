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
	//Read input text line as collection of lines
	JavaRDD<String> lines = sparkContext.textFile(args[0], 1);
	//Extract words from each line
	JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")));
	//Map each word as (word, 1)
	JavaPairRDD<String, Integer> wordMap = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
	//Reduce by word key and count them
	JavaPairRDD<String, Integer> wordCountPair = wordMap.reduceByKey((x, y) -> x + y);
	//Save wordcountpair as text file
	wordCountPair.saveAsTextFile(args[1]);
	sparkContext.stop();
    }
}
