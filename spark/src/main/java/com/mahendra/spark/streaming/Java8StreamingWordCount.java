/**
 * 
 */
package com.mahendra.spark.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * @author Mahendra J Kumar
 *
 */
public class Java8StreamingWordCount {

    public static void main(String[] args) {
	if(args.length != 3){
	    System.out.println("Usage: Java8StreamingWordCount <stream_source_host> <stream_source_port> <batch_windows_in_seconds");
	    System.exit(1);
	}
	SparkConf conf = new SparkConf().setAppName("Java8StreamingWordCount");
	JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(Integer.parseInt(args[2])));
	JavaReceiverInputDStream<String> lines = jsc.socketTextStream(args[0], Integer.parseInt(args[1]));
	JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")));
	JavaPairDStream<String, Integer> wordMap = words.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
	JavaPairDStream<String, Integer> wordCountPair = wordMap.reduceByKey((x, y) -> x + y);
	wordCountPair.print();
	jsc.start();
	jsc.awaitTermination();
	
    }
}
