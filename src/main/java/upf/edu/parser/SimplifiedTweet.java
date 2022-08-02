package upf.edu.parser;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.Optional;

public class SimplifiedTweet {

  private static final JsonParser parser = new JsonParser();

  private final long tweetId;			  // the id of the tweet ('id')
  private final String text;  		      // the content of the tweet ('text')
  private final long userId;			  // the user id ('user->id')
  private final String userName;		  // the user name ('user'->'name')
  private final String language;          // the language of a tweet ('lang')
  private final long timestampMs;		  // seconds from epoch ('timestamp_ms')

  public SimplifiedTweet(long tweetId, String text, long userId, String userName,
                         String language, long timestampMs) {

    // PLACE YOUR CODE HERE!
    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.language = language;
    this.timestampMs = timestampMs;
  }

  /**
   * Returns a {@link SimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link SimplifiedTweet}
   */
  public static Optional<SimplifiedTweet> fromJson(String jsonStr) {

    // PLACE YOUR CODE HERE!
    // We first check the Json is valid, if not we return an Empty Optional.
    // We could also use Option for je and jo
    try {
      JsonElement je = parser.parse(jsonStr);
      // JsonElement je = JsonParser.parseString(jsonStr);
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

      Optional<String> lang = Optional
              .ofNullable(jo.get("lang"))
              .map(JsonElement::getAsString);

      Optional<Long> timestamp = Optional
              .ofNullable(jo.get("timestamp_ms"))
              .map(JsonElement::getAsLong);

      if(id.isPresent() && text.isPresent() && user_id.isPresent() && user_name.isPresent() && lang.isPresent() && timestamp.isPresent()) {
        SimplifiedTweet simplifiedTweet = new SimplifiedTweet(
                id.get(),
                text.get(),
                user_id.get(),
                user_name.get(),
                lang.get(),
                timestamp.get());

        return Optional.of(simplifiedTweet);
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
}