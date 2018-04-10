package uga.tpspark.flickr

import java.net.URLDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FlickrExercise {
  def main(args: Array[String]): Unit = {
    // executing Spark on your machine, using 6 threads
    val conf = new SparkConf().setMaster("local[6]").setAppName("Flickr batch processing")
    val sc = new SparkContext(conf)

    // Control our logLevel. we can pass the desired log level as a string.
    // Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN
    sc.setLogLevel("WARN")
    try {
      val originalFlickrMetaData: RDD[String] = sc.textFile("flickrSample.txt")
      
      // 1. Display the 5 lines of the RDD and display the number of elements in the RDD.
      println("Question 1:\n")
      originalFlickrMetaData.take(5).foreach(println)
      println("\nNumber of ellments in the RDD: "+ originalFlickrMetaData.count())
      
      
      // 2. Transform the RDD[String] in RDD[Picture] using the Picture class. Only keep interesting
      // pictures having a valid country and tags. To check your program, display 5 elements.
      println("\nQuestion 2:\n")
      val splitFlickr = originalFlickrMetaData.map(line => line.split("\t")).map( x => (x) )
      val picRdd = splitFlickr.map(line => new Picture(line)).filter(x => x.hasValidCountry).filter(x => x.hasTags)
      picRdd.take(5).foreach(println)
      
        
      // 3. Now group these images by country (groupBy). Print the list of images corresponding to the
      // first country. What is the type of this RDD?
      println("\nQuestion 3:\n")
      val countryRdd = picRdd.groupBy(x => x.c)
      countryRdd.take(1).foreach(println)
      println("\nType of this RDD: RDD[(Country, Iterable[Picture])]")
      
      
      // 4. We now wish to process a RDD containing pairs in which the first element is a country, and
      // the second element is the list of tags used on pictures taken in this country. When a tag is
      // used on multiple pictures, it should appear multiple times in the list.
      println("\nQuestion 4:\n")
      val countryTagRdd = countryRdd.map(x => (x._1, x._2.flatMap(pic => pic.userTags).toList))
      countryTagRdd.take(5).foreach(println)
      
      // 5. We wish to avoid repetitions in the list of tags, and would rather like to have each tag associated
      // to its frequency. Hence, we want to build a RDD of type RDD[(Country, Map[String, Int])].
      println("\nQuestion 5:\n")
      val TagFrequencyRdd = countryTagRdd.map(x => (x._1, x._2.groupBy(identity).map(e => (e._1, e._2.length)))) 
      TagFrequencyRdd.foreach(println)
      
      
      // 6. There are often several ways to obtain a result. The method we used to compute the frequency
      // of tags in each country quickly reaches a state in which the size of the RDD is the number of
      // countries. This can limit the parallelism of the execution as the number of countries is often
      // quite small. Can you propose another way to reach the same result without reducing the size
      // of the RDD until the very end?
      println("\nQuestion 6:\n")
      println("This approach is different because now we create a frequency RDD for each picture.")
      println("Then, we use reduceByKey to add the frequencies together and end up with the same frequency Rdd as in question 5.")
      println("This makes it possible to paralellize for each picture until the last moment.\n")

      val countryTagRdd2 = picRdd.map(x => (x.c, x.userTags.toList))  
      val TagFrequencyRdd2 = countryTagRdd2.map(x => (x._1, x._2.groupBy(identity).map(e => (e._1, e._2.length)))) 
      val finalRdd = TagFrequencyRdd2.reduceByKey((x,y) => x ++ y.map{ case (key,value) => key -> (value + x.getOrElse(key,0)) })
      finalRdd.foreach(println)
      
    } catch {
      case e: Exception => throw e
    } finally {
      sc.stop()
    }
    println("done")
  }
}