  package com.cloudtv.spark.samples

  import org.apache.spark.sql.{SQLContext, SparkSession}
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.SparkContext._
  import org.apache.spark.rdd.RDD
  import org.apache.spark.streaming.StreamingContext
  import org.apache.spark.streaming.Seconds
  import org.apache.spark.streaming.Time
  import org.apache.spark.streaming.kafka.KafkaUtils

  object streamingdemo {

    def main(args: Array[String]): Unit = {
      println("Spark Streaming WordCount Sample Started ...")
      val spark = SparkSession.builder()
        .master(master = "local[*]")
        .appName("Spark Streaming Word Count Demo")
        .getOrCreate()

      val sc = spark.sparkContext
      //val sc = new SparkContext("spark://127.0.0.1:7077", "Spark Streaming Word Count Demo")

      val ssc = new StreamingContext(sc, Seconds(30))
      val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","wordcount-consumer-group", Map("wordCountTopic" -> 5))

      kafkaStream.print()
      kafkaStream.foreachRDD(rdd => {

        if (!rdd.partitions.isEmpty) {
          println(s"========= No. of Partitions: ${rdd.getNumPartitions} =========")

          rdd.map(item => println("First Value: " + item._1 + " --> " + item._2))
          val wordCounts = rdd.flatMap(line => line._2.split(" ")).map(word => (word, 1)).reduceByKey(_+_)

          println("Total words: " + wordCounts.count())

          println("All the words and occurrence count for each word: ")
          val wordCountsScala = wordCounts.collect()
          wordCountsScala.foreach(item => println("Word: " + item._1 + " --> " + "Occurrence count: " + item._2))

        }

      })

      ssc.start()
      ssc.awaitTermination()
      println("Spark Streaming WordCount Sample Completed.")
    }
  }
