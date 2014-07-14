/**
 * 
 */
package com.mahendra.spark.core;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.google.common.collect.Iterables;

import scala.Tuple2;

/**
 *  Java8 lambda expression Page rank class based on Spark page rank example
 * @author Mahendra J Kumar
 *
 */
public class Java8PageRank {

    public static void main(String[] args) {
	if (args.length != 3) {
	    System.err.println("Usage: Java8PageRank <input_file> <iterations> <output_file>");
	    System.exit(1);
	}
	SparkConf sparkConf = new SparkConf().setAppName("Java8PageRank");
	JavaSparkContext sc = new JavaSparkContext(sparkConf);
	// Read input text file containing URLs
	JavaRDD<String> lines = sc.textFile(args[0]);
	
	// Generate URL pairs
	JavaPairRDD<String, String> urlPairs = lines.mapToPair(line -> {
	    String[] parts = line.split("  ");
	    return new Tuple2<String, String>(parts[0], parts[1]);
	});
	
	// Group them by each URL key
	JavaPairRDD<String, Iterable<String>> groupedUrlPairs = urlPairs.distinct().groupByKey().cache();
	
	// For each URL assign an initial rank of 1.0
	JavaPairRDD<String, Double> urlRanks = groupedUrlPairs.mapValues(value -> 1.0);

	// Iterate and update url ranks
	for (int i = 0; i < Integer.parseInt(args[1]); i++) {
	    JavaPairRDD<String, Double> contribs = groupedUrlPairs.join(urlRanks).values().flatMapToPair(s -> {
		int urlCount = Iterables.size(s._1);
		List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
		for (String str : s._1) {
		    results.add(new Tuple2<String, Double>(str, s._2() / urlCount));
		}
		return results;
	    });
	    urlRanks = contribs.reduceByKey((x, y) -> (x + y)).mapValues(value -> 0.15 + 0.85 * value);
	}
	urlRanks.saveAsTextFile(args[2]);
    }
}
