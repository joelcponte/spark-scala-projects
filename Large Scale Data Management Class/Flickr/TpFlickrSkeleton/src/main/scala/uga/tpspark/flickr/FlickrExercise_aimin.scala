package uga.tpspark.flickr

import java.net.URLDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object FlickrExercise2 {
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
      println("\nQuestion 1:\n")
      originalFlickrMetaData.take(5).foreach(println)
      println("Number of lines: ", originalFlickrMetaData.count)
      
      
      // 2. Transform the RDD[String] in RDD[Picture] using the Picture class. Only keep interesting
      // pictures having a valid country and tags. To check your program, display 5 elements.
      println("\nQuestion 2:\n")
      
      val splitFlickr = originalFlickrMetaData.map(line => line.split("\t")).map( x => (x) )
      val splitFlickrPic = splitFlickr.map(line => new Picture(line)).filter(x => x.hasValidCountry).filter(x => x.hasTags)
      splitFlickrPic.take(5).foreach(println)
      
      // 3. Now group these images by country (groupBy). Print the list of images corresponding to the
      // first country. What is the type of this RDD?
      //      val countryRdd = picRdd.map(item => (item.c, item)).groupByKey()
      println("\nQuestion 3:\n")      
      val splitFlickrPicGrouped = splitFlickrPic.groupBy(x => x.c)
      splitFlickrPicGrouped.take(1).foreach(row => println(row._2))
      //--------------- Type of this RDD is: RDD[(Country, Iterable[Picture])]
      
      // 4. We now wish to process a RDD containing pairs in which the first element is a country, and
      // the second element is the list of tags used on pictures taken in this country. When a tag is
      // used on multiple pictures, it should appear multiple times in the list.
      println("\nQuestion 4:\n")
      val countryTagsRepeated = splitFlickrPic.map(x => (x.c, x.userTags)).reduceByKey((accum, n) => accum.union(n))
      countryTagsRepeated.foreach(row => println((row._1, row._2.toList)))
      
      
      // 5. We wish to avoid repetitions in the list of tags, and would rather like to have each tag associated
      // to its frequency. Hence, we want to build a RDD of type RDD[(Country, Map[String, Int])].
      println("\nQuestion 5:\n")
      val tagFrequencyRdd = countryTagsRepeated.map(item => (item._1, item._2.groupBy(identity).mapValues(_.size)))
      tagFrequencyRdd.foreach(row => println((row._1, row._2.toList)))
      
      // 6. There are often several ways to obtain a result. The method we used to compute the frequency
      // of tags in each country quickly reaches a state in which the size of the RDD is the number of
      // countries. This can limit the parallelism of the execution as the number of countries is often
      // quite small. Can you propose another way to reach the same result without reducing the size
      // of the RDD until the very end?
      println("\nQuestion 6:\n")
//      val countryTagRdd2 = picRdd.map(item => (item.c, item.userTags.toList))  
//      val TagFrequencyRdd2 = countryTagRdd2.map(item => (item._1, item._2.groupBy(identity).map(e => (e._1, e._2.length)))) 
//      val finalRdd = TagFrequencyRdd2.reduceByKey((x,y) => x++y)
      //      for ((k,v) <- finalRdd.first()._2) 
//        printf("key: %s, value: %s\n", k, v)
      
    } catch {
      case e: Exception => throw e
    } finally {
      sc.stop()
    }
    println("done")
  }
}