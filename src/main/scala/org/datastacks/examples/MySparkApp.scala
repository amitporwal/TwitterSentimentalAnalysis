package org.datastacks.examples

import SentimentUtils._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.elasticsearch.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


import java.util.Date
import java.text.SimpleDateFormat
import java.util.Locale

import scala.util.Try
object MySparkApp {
      def main(args: Array[String]) {

     if (args.length < 4) {
       System.err.println("Usage: TwitterSentimentAnalysis <consumer key> <consumer secret> " +
         "<access token> <access token secret> [<filters>]")
       System.exit(1)
     }

     val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
     val filters = args.takeRight(args.length - 4)

     // set twitter oAuth keys
     //Set the system properties so that Twitter4j library used by twitter stream
     // Use them to generate OAuth credentials
     System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
     System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
     System.setProperty("twitter4j.oauth.accessToken", accessToken)
     System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

     val conf = new SparkConf().setAppName("TwitterSentimentAnalysis").setMaster("local[4]")
     
     // Create a DStream for every 5 seconds
     val ssc = new StreamingContext(conf, Seconds(5))

     // Get json object from twitter stream
     // None is passed as we are using twitter4j library to set twitter credential.
     val tweets = TwitterUtils.createStream(ssc, None, filters)
     
     val englishtweets = tweets.filter(_.getLang() == "en") 
     englishtweets.print()
    // englishtweets.saveAsTextFiles("output", "json")
    // englishtweets.foreachRDD{(rdd, time) =>
     //  rdd.saveAsTextFile("output")
    // }*/
       englishtweets.foreachRDD{(rdd, time) =>
       rdd.map(t => {
          Map(
           "user"-> t.getUser.getScreenName,
           "created_at" -> t.getCreatedAt.getTime.toString,
           "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
           "text" -> t.getText,
           "hashtags" -> t.getHashtagEntities.map(_.getText),
           "retweet" -> t.getRetweetCount,
           "language" -> t.getLang.toString(),
           "sentiment" -> detectSentiment(t.getText).toString
         )
       }).saveAsTextFile("output.txt")
    } 
     ssc.start()
     ssc.awaitTermination()
  }
}