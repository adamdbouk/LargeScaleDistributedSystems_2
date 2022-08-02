package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import upf.edu.parser.SimplifiedTweet;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class TwitterLanguageFilterApp {
    public static void main( String[] args ) {
        long start = System.currentTimeMillis(); // Benchmarking
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String input = argsList.get(2);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("TwitterLanguageFilterApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input
        JavaRDD<String> jsonTweets = sparkContext.textFile(input);

        JavaRDD<SimplifiedTweet> tweets = jsonTweets
                .map(SimplifiedTweet::fromJson) // Obtaining Optional<SimplifiedTweet>
                .filter(Optional::isPresent) // Obtaining present tweet, deleting optional.empty() tweets
                .map(Optional::get) // Obtaining SimplifiedTweet instances
                .filter(sTweet -> sTweet.getLanguage().equals(language)); // Filtering by lang

        // Coalesce(1) to get the output in a single file
        tweets.coalesce(1).saveAsTextFile(outputFile);

        // Benchmarking: total time
        long stop = System.currentTimeMillis();
        System.out.println("Done in ms: " + (stop-start));

    }
}
