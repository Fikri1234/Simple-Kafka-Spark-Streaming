package com.example.project;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import scala.Tuple2;

@Component
public class ConsumeKafkaStream {

	private final Logger log = LoggerFactory.getLogger(ConsumeKafkaStream.class);

	private static final String dataPath = "D:\\STS_Develop\\";

	@Bean
	public SparkConf sparkConf() {

		return new SparkConf().setAppName("Simple App Kafka Spark Streaming")
				.setSparkHome("D:/STS_Develop/spark-2.4.0-bin-hadoop2.7").setMaster("local[*]") // The Spark Context we
																								// have created here has
																								// been allocated all
																								// the available local
																								// processors, hence the
																								// *.
				.set("spark.driver.allowMultipleContexts", "true");
	}

	@Bean
	public JavaSparkContext javaSparkContext() {
		log.debug("============= start jsc =============");
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf()));
		return jsc;
	}

	@Bean
	public void confSparkStreaming() {

		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("group.id", "streams-consumer-group");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("streams-input");

		JavaStreamingContext jssc = new JavaStreamingContext(javaSparkContext(), new Duration(60000));

		
		//----------------------------------------------------
		JavaInputDStream<ConsumerRecord<String, String>> dataLines = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferBrokers(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		JavaDStream<String> fileStream = jssc.textFileStream(dataPath + "spark-input.txt");
		fileStream.print();

		JavaDStream<String> logdata = dataLines.map(ConsumerRecord::value);

		JavaPairDStream<String, Integer> wordCount = logdata
				.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
				.reduceByKey((Function2<Integer, Integer, Integer>) (i1, i2) -> i1 + i2);

		wordCount.foreachRDD((v1, v2) -> {

		});
		//--------------------------------------------------
		
		dataLines.map(record -> record.value().toString()).print();

		// Start the computation
		jssc.start();

		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Bean
	public void readFile() {

		log.debug("============= start read file =============");
		
		 // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
                                        .setMaster("local[2]").set("spark.executor.memory","2g");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> distFile = sc.textFile(dataPath + "spark-input.txt");

		JavaRDD<Integer> lineLengths = distFile.map(s -> s.length());
		/*
		 * JavaRDD<Integer> lineLengths = distFile.map(new Function<String, Integer>() {
		 *//**
			* 
			*//*
				 * private static final long serialVersionUID = 1L;
				 * 
				 * public Integer call(String s) { return s.length(); } });
				 */

		int totalLength = lineLengths.reduce((a, b) -> a + b);
		/*
		 * int totalLength = lineLengths.reduce(new Function2<Integer, Integer,
		 * Integer>() {
		 *//**
			* 
			*//*
				 * private static final long serialVersionUID = 1L;
				 * 
				 * public Integer call(Integer a, Integer b) { return a + b; } });
				 */

		log.debug("****************** total length: " + totalLength);

		// collect RDD for printing
		for (String line : distFile.collect()) {
			System.out.println(line);
		}
		
		JavaPairRDD<String, Integer> pairs = distFile.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		counts.sortByKey();
		counts.saveAsTextFile(dataPath + "spark-output.txt");

		distFile.saveAsTextFile(dataPath + "spark-output.txt");

		sc.close();
	}

}
