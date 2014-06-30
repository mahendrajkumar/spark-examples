/**
 * 
 */
package com.mahendra.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Mahendra J Kumar
 *
 */
public class Java8TextSearch {
    
    public static void main(String[] args) {
	if (args.length != 3) {
	    System.err.println("Usage: Java8TextSearch <input_file> <search_word> <output_file>");
	    System.exit(1);
	}
	SparkConf conf = new SparkConf().setAppName("TextSearch");
	JavaSparkContext sparkContext = new JavaSparkContext(conf);
	//Read input text line as collection of lines
	JavaRDD<String> lines = sparkContext.textFile(args[0], 1);
	//filter lines based on search word
	JavaRDD<String> filteredLines = lines.filter(line -> line.contains(args[1]));
	//Save the filtered lines into a text file
	filteredLines.saveAsTextFile(args[2]);
	//Count the number of lines
	long count = filteredLines.count();
    }
}
