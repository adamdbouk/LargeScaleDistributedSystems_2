package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import upf.edu.parser.ExtendedSimplifiedTweet;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class MostRetweetedApp {
    public static void main( String[] args ) {
        long start = System.currentTimeMillis(); // Benchmarking
        List<String> argsList = Arrays.asList(args);
        String outputFile = argsList.get(0);
        String input = argsList.get(1);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("TwitterLanguageFilterApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input
        JavaRDD<String> jsonTweets = sparkContext.textFile(input);

        // We save the tweets so we are not doing this many times
        JavaRDD<ExtendedSimplifiedTweet> tweets = jsonTweets
                .map(ExtendedSimplifiedTweet::fromJson)
                .filter(Optional::isPresent)
                .map(Optional::get);

        // We get total retweets for each user (including those who have no tweets in the dataset)
        // At the end it has the form (tweetId, # of retweets)
        JavaPairRDD<Long, Integer> retweetsUser = tweets
                .filter(ExtendedSimplifiedTweet::isRetweeted)
                .map(ExtendedSimplifiedTweet::getRetweetedUserId)
                .mapToPair(t -> new Tuple2<>(t, 1))
                .reduceByKey(Integer::sum);  // Counting number of retweets

        // Retweets for each tweet, we also add the userId so we can later join with retweets per user
        JavaPairRDD<Tuple2<Long, Long>, Integer> retweetsTweet = tweets
                .filter(ExtendedSimplifiedTweet::isRetweeted)
                .map(t -> new Tuple2<>(t.getRetweetedTweetId(), t.getRetweetedUserId())) // We now have the retweeted tweet ids
                .mapToPair(t -> new Tuple2<>(t, 1))
                .reduceByKey(Integer::sum);

        // First 3 lines: We obtain (tweetId, (userid, #RtUser, #RtTweet)) so we can join them with tweets in db
        JavaPairRDD<ExtendedSimplifiedTweet, String> top10Tweets = retweetsTweet
                .mapToPair(a -> new Tuple2<>(a._1._2, new Tuple2<>(a._1._1, a._2)))
                .join(retweetsUser)
                .mapToPair(t -> new Tuple2<>(t._2._1._1, new Tuple3<>(t._1, t._2._2, t._2._1._2)))
                .join(tweets.mapToPair(tweet -> new Tuple2<>(tweet.getTweetId(), tweet))) // Joining with tweets in db.
                .mapToPair(t -> new Tuple2<>(t._2._1._1(), new Tuple3<>(t._2._2, t._2._1._3(), t._2._1._2())) )
                .reduceByKey((a,b) -> a._2() >= b._2() ? a : b) // We get the most retweeted tweet for each user
                .mapToPair(c -> new Tuple2<>(c._2._3(), new Tuple2<>(c._2._1(), c._2._2())))// (#rtuser, (tweet, #rtTweet))
                .sortByKey(false) // We sort by most retweeted users
                .mapToPair(m -> new Tuple2<>(m._2._1(), " Tweet retweets: " + m._2._2 + ", User retweets: " + m._1())); // (twit, String("twit rt, user rt"))

        // We write in the output file the first 10 tweets from the JavaRDD
        sparkContext.parallelize(top10Tweets.take(10)).coalesce(1).saveAsTextFile(outputFile);

        // Benchmarking: total time
        long stop = System.currentTimeMillis();
        System.out.println("Done in ms: " + (stop-start));
    }
}
