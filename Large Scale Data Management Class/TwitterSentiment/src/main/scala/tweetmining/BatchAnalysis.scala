package tweetmining

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object BatchAnalysis {
  val conf = new SparkConf().setMaster("local[6]").setAppName("Spark batch processing")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    println("Twitter data batch processing")
    val tweets: RDD[String] = sc.textFile("1Mtweets_en.txt")
    //add your code here
    // 1
    val trumpMentions = tweets.filter(x => x.contains("@realdonaldtrump")).count()
    println("@realdonaldtrump mentions:")
    println(trumpMentions)
    println("\n")
    
    // 2
    val sentimentsMap = tweets.map(x => (TweetUtilities.getSentiment(x), x))
    println("Sentiments Map:")
    sentimentsMap.take(5).foreach(println)
    println("\n")
    
    // 3
    val tagsSentiment = sentimentsMap.mapValues(x => TweetUtilities.getMentions(x))
    println("Mentions Sentiments:")
    tagsSentiment.take(5).foreach(println)
    println("\n")
    
    // 4
    println("Most positive / negative mentions:")
    val lowestTweets = tagsSentiment.takeOrdered(5)(Ordering[Double].on(x=>x._1))
    lowestTweets.take(5).foreach(println)
    val highestTweets = tagsSentiment.top(5)(Ordering[Double].on(x=>x._1))
    highestTweets.take(5).foreach(println)
  }
}