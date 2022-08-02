package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.parser.ExtendedSimplifiedTweet;

import java.util.*;

public class BiGramsApp {
    public static void main( String[] args ) {
        long start = System.currentTimeMillis(); // Benchmarking
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String input = argsList.get(2);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGramsApp");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // Load input
        JavaRDD<String> jsonTweets = sparkContext.textFile(input);


        JavaPairRDD<Tuple2<String, String>, Integer> tweetsBiGrams = jsonTweets
                .map(ExtendedSimplifiedTweet::fromJson) // Parsing each tweet to an ExtendedSimplifiedTweet
                .filter(Optional::isPresent) // Filter only valid tweets
                .map(Optional::get)
                .filter(tweet -> !tweet.isRetweeted() && tweet.getLanguage().equals(language)) // original and lang
                .map(ExtendedSimplifiedTweet::getText)
                .flatMap(s -> getBiGrams(s).iterator()) // We get the biGrams
                .mapToPair(t -> new Tuple2<>(t, 1))
                .reduceByKey(Integer::sum) // Reduce to count the # of occurrences
                .mapToPair(Tuple2::swap) // Swapping key and value to be able to sort
                .sortByKey(false) // We sort the dataset descending by # of occurrences
                .mapToPair(Tuple2::swap); // We swap again to get the biGram as the key

        // We take 10 first elements, but as take(n) returns a list we parallelize it into an RDD which we will write in output.
        sparkContext.parallelize(tweetsBiGrams.take(10)).coalesce(1).saveAsTextFile(outputFile);

        // Benchmarking: total time
        long stop = System.currentTimeMillis();
        System.out.println("Done in ms: " + (stop-start));

    }
    private static String normalize(String word) {
        String[] symbols = {",", ".", ";", ":", "-"};
        String normWord = word
                .trim()
                .toLowerCase()
                .replaceAll("\n", "") // Deleting jump lines between words
                .replaceAll("\r", "");

        // Deleting all symbols surrounding the word
        //while (Arrays.stream(symbols).anyMatch(word::endsWith) || Arrays.stream(symbols).anyMatch(word::startsWith)) {
            if (Arrays.stream(symbols).anyMatch(word::endsWith)) {
                normWord = word.substring(0, word.length() - 1);
            }
            if (Arrays.stream(symbols).anyMatch(word::startsWith)) {
                normWord = word.substring(1);
            }
        //}
        return normWord;
    }

    private static List<Tuple2<String, String>> getBiGrams(String text) {
        // Splitting the string to get all the words
        List<String> words = new ArrayList<>(Arrays.asList(text.split("[ ]")));
        // removing all empty strings
        words.removeAll(Collections.singleton(""));
        // bigrams will store all the biGrams in a tuple
        List<Tuple2<String, String>> bigrams = new ArrayList<>();
        for (int i=0; i<words.size()-1; i++) {
            Tuple2<String, String> bigram = new Tuple2<>(normalize(words.get(i)), normalize(words.get(i+1)));
            bigrams.add(bigram);
        }
        return bigrams;
    }
}
