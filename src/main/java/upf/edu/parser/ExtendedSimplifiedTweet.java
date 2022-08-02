package upf.edu.parser;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.Optional;

public class ExtendedSimplifiedTweet implements Serializable {

  private static final JsonParser parser = new JsonParser();

  private final long tweetId; // the id of the tweet (’id’)
  private final String text; // the content of the tweet (’text’)
  private final long userId; // the user id (’user->id’)
  private final String userName; // the user name (’user’->’name’)
  private final long followersCount; // the number of followers (’user’->’followers_count’)
  private final String language; // the language of a tweet (’lang’)
  private final boolean isRetweeted; // is it a retweet? (the object ’retweeted_status’ exists?)
  private final Long retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
  private final Long retweetedTweetId; // [if retweeted] (’retweeted_status’->’id’)
  private final long timestampMs; // seconds from epoch (’timestamp_ms’)

  public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName,
                                 long followersCount, String language, boolean isRetweeted,
                                 Long retweetedUserId, Long retweetedTweetId, long timestampMs) {

    // PLACE YOUR CODE HERE!
    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.followersCount = followersCount;
    this.language = language;
    this.isRetweeted = isRetweeted;
    this.retweetedUserId = retweetedUserId;
    this.retweetedTweetId = retweetedTweetId;
    this.timestampMs = timestampMs;
  }

  /**
   * Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
   */
  public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {

    // PLACE YOUR CODE HERE!
    // We first check the Json is valid, if not we return an Empty Optional.
    // We could also use Option for je and jo
    try {
      JsonElement je = parser.parse(jsonStr);
      JsonObject jo = je.getAsJsonObject();

      Optional<Long> id = Optional
              .ofNullable(jo.get("id"))
              .map(JsonElement::getAsLong); // Same as j -> j.getAsLong()

      Optional<String> text = Optional
              .ofNullable(jo.get("text"))
              .map(JsonElement::getAsString);

      // <JsonElement> user_id = Optional.ofNullable(jo.get("user->id"));
      Optional<Long> user_id = Optional
              .ofNullable(jo.get("user"))
              .map(userJson -> userJson.getAsJsonObject().get("id"))
              .map(JsonElement::getAsLong);

      // Optional<String> user_name = Optional.ofNullable(jo.get("user->name"));
      Optional<String> user_name = Optional
              .ofNullable(jo.get("user"))
              .map(userJson -> userJson.getAsJsonObject().get("name"))
              .map(JsonElement::getAsString);

      // (’user’->’followers_count’)
      Optional<Long> followersCount = Optional
              .ofNullable(jo.get("user"))
              .map(userJson -> userJson.getAsJsonObject().get("followers_count"))
              .map(JsonElement::getAsLong);

      Optional<String> lang = Optional
              .ofNullable(jo.get("lang"))
              .map(JsonElement::getAsString);

      // is it a retweet? (the object ’retweeted_status’ exists?)
      Optional<JsonElement> rt_status_obj = Optional
              .ofNullable(jo.get("retweeted_status"));

      boolean rt_status = rt_status_obj.isPresent();

      // [if retweeted] (’retweeted_status’->’user’->’id’)
      Optional<Long> rt_user_id = Optional
              .ofNullable(jo.get("retweeted_status"))
              .map(rt_st -> rt_st.getAsJsonObject().get("user"))
              .map(rt_usr -> rt_usr.getAsJsonObject().get("id"))
              .map(JsonElement::getAsLong);

      // [if retweeted] (’retweeted_status’->’id’)
      Optional<Long> rt_tweet_id = Optional
              .ofNullable(jo.get("retweeted_status"))
              .map(rt_st -> rt_st.getAsJsonObject().get("id"))
              .map(JsonElement::getAsLong);

      Optional<Long> timestamp = Optional
              .ofNullable(jo.get("timestamp_ms"))
              .map(JsonElement::getAsLong);

      if(id.isPresent() && text.isPresent() && user_id.isPresent() && user_name.isPresent() && followersCount.isPresent() && lang.isPresent() && timestamp.isPresent()) {
        if (rt_status && rt_user_id.isPresent() && rt_tweet_id.isPresent()) {
          ExtendedSimplifiedTweet simplifiedTweet = new ExtendedSimplifiedTweet(
                  id.get(),
                  text.get(),
                  user_id.get(),
                  user_name.get(),
                  followersCount.get(),
                  lang.get(),
                  true,
                  rt_user_id.get(),
                  rt_tweet_id.get(),
                  timestamp.get());
          return Optional.of(simplifiedTweet);
        } else if (!rt_status){
          ExtendedSimplifiedTweet simplifiedTweet = new ExtendedSimplifiedTweet(
                  id.get(),
                  text.get(),
                  user_id.get(),
                  user_name.get(),
                  followersCount.get(),
                  lang.get(),
                  false,
                  null,
                  null,
                  timestamp.get());
          return Optional.of(simplifiedTweet);
        }
        return Optional.empty();

      } else {
        return Optional.empty();
      }
    } catch (Exception ex) { // The Json is not a valid Json, we just return an empty Optional.
      // ex.printStackTrace(); // In case we want to see the error.
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
      // Overriding how SimplifiedTweets are printed in console or the output file
      // The following line produces valid JSON as output
    return new Gson().toJson(this);
  }

  // GETTERS
  public long getTweetId() {
    return this.tweetId;
  }
  public String getText() {
    return this.text;
  }
  public long getUserId() {
    return this.userId;
  }
  public String getUserName() {
    return this.userName;
  }
  public String getLanguage() {
    return this.language;
  }
  public long getTimestampMs() {
    return this.timestampMs;
  }
  public long getFollowersCount() {
    return followersCount;
  }
  public Long getRetweetedTweetId() {
    return retweetedTweetId;
  }
  public boolean isRetweeted() {
    return isRetweeted;
  }
  public Long getRetweetedUserId() {
    return retweetedUserId;
  }
}